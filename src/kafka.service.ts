import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import {
  trace,
  context,
  SpanKind,
  SpanStatusCode,
  ROOT_CONTEXT,
} from '@opentelemetry/api';
import { RedisService } from './redis.service';
import { instrumentProducer, runInstrumented } from './kafka-otel';

// @confluentinc/kafka-javascript ships a KafkaJS-compatible API surface.
// eslint-disable-next-line @typescript-eslint/no-var-requires
const { Kafka, logLevel } = require('@confluentinc/kafka-javascript').KafkaJS;

// Topics that mirror the real crowncoins-production-missions Kafka usage.
const TOPICS = ['missions-progress', 'rank'];
const GROUP_ID = 'missions-progress-workers';

// Tracer for app-domain (non-Kafka) spans, e.g. the Monetization background txns.
const tracer = trace.getTracer('missions-app', '1.0.0');

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
    // otherwise dumps multi-line JS objects to stdout (the log-collection agent
    // ingests each physical line as a separate record, fragmenting the Logs view).
    this.kafka = new Kafka({
      kafkaJS: { clientId: 'missions-sim', brokers, logLevel: logLevel.NOTHING },
    });
    // Connect in the background with retry so the HTTP server boots regardless
    // of broker availability (HTTP + Redis traces keep flowing either way).
    void this.connectProducer();
    void this.connectConsumer();
  }

  private async connectProducer() {
    // instrumentProducer() wraps .send() to emit PRODUCER spans automatically.
    this.producer = instrumentProducer(this.kafka.producer());
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
      kafkaJS: { groupId: GROUP_ID, fromBeginning: false },
    });
    for (;;) {
      try {
        await this.consumer.connect();
        await this.consumer.subscribe({ topics: TOPICS });
        // runInstrumented() wraps each message in a top-level CONSUMER span + link.
        await runInstrumented(this.consumer, {
          groupId: GROUP_ID,
          eachMessage: (payload: any) => this.handleMessage(payload),
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
   * Publish a message. The PRODUCER span + header propagation are handled by the
   * instrumented producer — no span code here.
   */
  async produce(topic: string, payload: Record<string, unknown>): Promise<void> {
    if (!this.producerReady) {
      log('WARN', 'produce skipped, producer not ready', { topic });
      return;
    }
    try {
      await this.producer.send({
        topic,
        messages: [{ value: JSON.stringify(payload) }],
      });
    } catch (e: any) {
      log('ERROR', 'produce failed', { topic, error: String(e?.message || e) });
    }
  }

  /**
   * Pure business logic — runs inside the CONSUMER span created by the wrapper,
   * so Redis writes nest under it automatically. No span code here.
   */
  private async handleMessage(payload: any): Promise<void> {
    const { topic, message } = payload;
    const data = safeJson(message.value);

    if (topic === 'missions-progress' && data?.userId) {
      await this.redis.applyProgress(
        String(data.userId),
        String(data.mission || 'daily_login'),
        Number(data.value ?? 0),
      );
      // Non-web background transactions that mirror the real service's
      // "Monetization" transaction type (separate trace roots).
      await this.runMonetizationTxn('Monetization/missions-progress-process', 5);
      await this.runMonetizationTxn('Monetization/missions-progress-build-lookups', 1);
    }
  }

  /**
   * Non-web background transaction. Started from ROOT_CONTEXT (its own trace
   * root) with span.kind=INTERNAL and no http/messaging attributes, so Coralogix
   * classifies it as a non-web transaction named after the span. `weight` scales
   * the simulated work so "process" dominates "build-lookups" (~85/15 by time).
   */
  private async runMonetizationTxn(name: string, weight: number): Promise<void> {
    const span = tracer.startSpan(name, { kind: SpanKind.INTERNAL }, ROOT_CONTEXT);
    try {
      await context.with(trace.setSpan(ROOT_CONTEXT, span), async () => {
        await this.redis.getProgress(`bg-${Math.floor(Math.random() * 5000)}`);
        await sleep(weight);
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
