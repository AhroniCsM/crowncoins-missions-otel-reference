import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import {
  trace,
  context,
  propagation,
  SpanKind,
  SpanStatusCode,
  ROOT_CONTEXT,
  Span,
} from '@opentelemetry/api';
import { RedisService } from './redis.service';

// @confluentinc/kafka-javascript ships a KafkaJS-compatible API surface.
// eslint-disable-next-line @typescript-eslint/no-var-requires
const { Kafka, logLevel } = require('@confluentinc/kafka-javascript').KafkaJS;

const tracer = trace.getTracer('missions-kafka-manual', '1.0.0');

// Topics that mirror the real crowncoins-production-missions Kafka usage.
const TOPICS = ['missions-progress', 'rank'];

function log(level: string, message: string, extra: Record<string, unknown> = {}) {
  // eslint-disable-next-line no-console
  console.log(JSON.stringify({ level, component: 'kafka', message, ...extra }));
}

@Injectable()
export class KafkaService implements OnModuleInit, OnModuleDestroy {
  private kafka: any;
  private producer: any;
  private consumer: any;
  private producerReady = false;

  constructor(private readonly redis: RedisService) {}

  onModuleInit() {
    const brokers = (process.env.KAFKA_BOOTSTRAP || 'kafka:9092').split(',');
    // logLevel.NOTHING silences the librdkafka client's internal logger, which
    // otherwise dumps multi-line JS objects to stdout. The log-collection agent
    // ingests each physical line as a separate record, fragmenting the Logs view
    // into "{", "message: ...", "}" pieces. We log produce/consume failures
    // ourselves as single-line JSON instead.
    this.kafka = new Kafka({
      kafkaJS: { clientId: 'missions-sim', brokers, logLevel: logLevel.NOTHING },
    });
    // Connect in the background with retry so the HTTP server boots regardless
    // of broker availability (HTTP + Redis traces keep flowing either way).
    void this.connectProducer();
    void this.connectConsumer();
  }

  private async connectProducer() {
    this.producer = this.kafka.producer();
    for (;;) {
      try {
        await this.producer.connect();
        this.producerReady = true;
        log('INFO', 'producer connected');
        return;
      } catch (e: any) {
        log('WARN', 'producer connect failed, retrying', { error: String(e?.message || e) });
        await sleep(3000);
      }
    }
  }

  private async connectConsumer() {
    this.consumer = this.kafka.consumer({
      kafkaJS: { groupId: 'missions-progress-workers', fromBeginning: false },
    });
    for (;;) {
      try {
        await this.consumer.connect();
        await this.consumer.subscribe({ topics: TOPICS });
        await this.consumer.run({
          eachMessage: async (payload: any) => this.handleMessage(payload),
        });
        log('INFO', 'consumer connected & running', { topics: TOPICS });
        return;
      } catch (e: any) {
        log('WARN', 'consumer connect failed, retrying', { error: String(e?.message || e) });
        await sleep(3000);
      }
    }
  }

  /**
   * PRODUCER span — required Coralogix Kafka attributes:
   *   span.kind = PRODUCER, messaging.system = kafka, messaging.destination(.name) = topic
   * Also injects W3C traceparent into the message headers so the consumer can
   * create a span LINK back to this producer (async flows break parent/child).
   */
  async produce(topic: string, payload: Record<string, unknown>): Promise<void> {
    if (!this.producerReady) {
      log('WARN', 'produce skipped, producer not ready', { topic });
      return;
    }
    const span = tracer.startSpan(`Produce Topic ${topic}`, {
      kind: SpanKind.PRODUCER,
      attributes: {
        'messaging.system': 'kafka',
        'messaging.destination': topic,
        'messaging.destination.name': topic,
        'messaging.destination_kind': 'topic',
        'messaging.operation': 'publish',
      },
    });
    const ctx = trace.setSpan(context.active(), span);
    const headers: Record<string, string> = {};
    propagation.inject(ctx, headers); // -> headers.traceparent

    try {
      await context.with(ctx, async () => {
        const result = await this.producer.send({
          topic,
          messages: [{ value: JSON.stringify(payload), headers }],
        });
        const partition = result?.[0]?.partition;
        if (partition !== undefined) {
          span.setAttribute('messaging.kafka.partition', partition);
          span.setAttribute('messaging.kafka.destination.partition', partition);
        }
      });
    } catch (e: any) {
      span.recordException(e);
      span.setStatus({ code: SpanStatusCode.ERROR, message: String(e?.message || e) });
      log('ERROR', 'produce failed', { topic, error: String(e?.message || e) });
    } finally {
      span.end();
    }
  }

  /**
   * CONSUMER span — top-level span (its own trace root) carrying the same
   * required Kafka attributes, PLUS a span LINK to the producer span extracted
   * from the message headers. Per Coralogix guidance the link MUST be on the
   * top-level consumer span (not a nested child) for the service map to connect
   * producer -> kafka -> consumer. This is exactly what cx498 is missing today.
   */
  private async handleMessage(payload: any): Promise<void> {
    const { topic, partition, message } = payload;

    // Extract producer context from headers -> build a span link.
    const carrier: Record<string, string> = {};
    for (const [k, v] of Object.entries(message.headers || {})) {
      if (v != null) carrier[k] = Buffer.isBuffer(v) ? v.toString('utf8') : String(v);
    }
    const producerCtx = propagation.extract(ROOT_CONTEXT, carrier);
    const linkedSpanCtx = trace.getSpan(producerCtx)?.spanContext();

    const span: Span = tracer.startSpan(
      `Consume Topic ${topic}`,
      {
        kind: SpanKind.CONSUMER,
        attributes: {
          'messaging.system': 'kafka',
          'messaging.destination': topic,
          'messaging.destination.name': topic,
          'messaging.destination_kind': 'topic',
          'messaging.operation': 'process',
          'messaging.kafka.partition': partition,
          'messaging.consumer.group': 'missions-progress-workers',
        },
        links: linkedSpanCtx ? [{ context: linkedSpanCtx }] : [],
      },
      ROOT_CONTEXT, // start as a fresh root so it is a true top-level consumer span
    );

    try {
      await context.with(trace.setSpan(ROOT_CONTEXT, span), async () => {
        const data = safeJson(message.value);
        if (topic === 'missions-progress' && data?.userId) {
          // Redis writes here become child spans of the consumer span.
          await this.redis.applyProgress(
            String(data.userId),
            String(data.mission || 'daily_login'),
            Number(data.value ?? 0),
          );
        }
      });
      // Emit the non-web background transactions that mirror the real service's
      // "Monetization" transaction type. These run as their own trace roots so
      // Coralogix lists them as separate non-web transactions.
      if (topic === 'missions-progress') {
        await this.runMonetizationTxn('Monetization/missions-progress-process', 5);
        await this.runMonetizationTxn('Monetization/missions-progress-build-lookups', 1);
      }
    } catch (e: any) {
      span.recordException(e);
      span.setStatus({ code: SpanStatusCode.ERROR, message: String(e?.message || e) });
      log('ERROR', 'consume failed', { topic, error: String(e?.message || e) });
    } finally {
      span.end();
    }
  }

  /**
   * Non-web background transaction. Started from ROOT_CONTEXT (its own trace
   * root) with span.kind=INTERNAL and no http/messaging attributes, so Coralogix
   * classifies it as a non-web transaction named after the span — e.g.
   * "Monetization/missions-progress-process". `weight` scales the simulated work
   * so "process" dominates "build-lookups" (~85/15 by time consumed).
   */
  private async runMonetizationTxn(name: string, weight: number): Promise<void> {
    const span = tracer.startSpan(name, { kind: SpanKind.INTERNAL }, ROOT_CONTEXT);
    try {
      await context.with(trace.setSpan(ROOT_CONTEXT, span), async () => {
        // a touch of Redis so the transaction has a realistic child span
        await this.redis.getProgress(`bg-${Math.floor(Math.random() * 5000)}`);
        await sleep(weight); // process (5ms) ~6x build-lookups (1ms)
      });
    } catch (e: any) {
      span.recordException(e);
      span.setStatus({ code: SpanStatusCode.ERROR, message: String(e?.message || e) });
    } finally {
      span.end();
    }
  }

  async onModuleDestroy() {
    try {
      await this.consumer?.disconnect();
    } catch {
      /* ignore */
    }
    try {
      await this.producer?.disconnect();
    } catch {
      /* ignore */
    }
  }
}

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

function safeJson(v: any): any {
  try {
    return JSON.parse(Buffer.isBuffer(v) ? v.toString('utf8') : String(v));
  } catch {
    return null;
  }
}
