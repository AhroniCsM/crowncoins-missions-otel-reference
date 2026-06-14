/**
 * Reusable OpenTelemetry wrapper for @confluentinc/kafka-javascript (KafkaJS API).
 *
 * Why this exists: librdkafka-based clients (@confluentinc/kafka-javascript) are NOT
 * covered by any OTel auto-instrumentation, so Kafka spans must be created manually.
 * Instead of scattering span code through the app, wrap the producer/consumer ONCE
 * here; business code then calls `producer.send(...)` / `runInstrumented(consumer, ...)`
 * and gets correct, Coralogix-compatible spans for free:
 *
 *   - PRODUCER span: span.kind=PRODUCER, messaging.system=kafka,
 *     messaging.destination.name=<topic>; injects W3C traceparent into headers.
 *   - top-level CONSUMER span: same attributes + a span LINK back to the producer
 *     (async flows break parent/child — the link is what connects them in the
 *     Service Map). Must be on the top-level consumer span, which is what this does.
 *
 * Drop this file into any service using @confluentinc/kafka-javascript and wrap
 * the producer/consumer at creation time — no other code changes required.
 */
import {
  trace,
  context,
  propagation,
  SpanKind,
  SpanStatusCode,
  ROOT_CONTEXT,
} from '@opentelemetry/api';

const tracer = trace.getTracer('kafka-otel', '1.0.0');

interface ProducerLike {
  send(record: any): Promise<any>;
}
interface ConsumerLike {
  run(config: { eachMessage: (payload: any) => Promise<void> }): Promise<any>;
}

function messagingAttrs(topic: string) {
  return {
    'messaging.system': 'kafka',
    'messaging.destination': topic,
    'messaging.destination.name': topic,
    'messaging.destination_kind': 'topic',
  };
}

/**
 * Wrap a producer so every `.send()` emits a PRODUCER span and propagates the
 * trace context into the message headers. Returns the same producer instance.
 */
export function instrumentProducer<T extends ProducerLike>(producer: T): T {
  const originalSend = producer.send.bind(producer);
  producer.send = async (record: any) => {
    const topic = record.topic;
    const span = tracer.startSpan(`Produce Topic ${topic}`, {
      kind: SpanKind.PRODUCER,
      attributes: { ...messagingAttrs(topic), 'messaging.operation': 'publish' },
    });
    const ctx = trace.setSpan(context.active(), span);
    // Inject W3C traceparent into every message's headers for the consumer link.
    record.messages = (record.messages || []).map((m: any) => {
      const headers = { ...(m.headers || {}) };
      propagation.inject(ctx, headers);
      return { ...m, headers };
    });
    try {
      const result = await context.with(ctx, () => originalSend(record));
      const partition = result?.[0]?.partition;
      if (partition !== undefined) {
        span.setAttribute('messaging.kafka.partition', partition);
      }
      return result;
    } catch (e: any) {
      span.recordException(e);
      span.setStatus({ code: SpanStatusCode.ERROR, message: String(e?.message || e) });
      throw e;
    } finally {
      span.end();
    }
  };
  return producer;
}

/**
 * Run a consumer with each message wrapped in a top-level CONSUMER span that
 * carries the required Kafka attributes and a span LINK to the producer span
 * extracted from the message headers. `handler` only sees the message — no span
 * code. Child spans created inside the handler nest under the consumer span.
 */
export function runInstrumented(
  consumer: ConsumerLike,
  opts: { groupId?: string; eachMessage: (payload: any) => Promise<void> },
): Promise<any> {
  return consumer.run({
    eachMessage: async (payload: any) => {
      const { topic, partition, message } = payload;

      // Extract the producer context from headers -> span link.
      const carrier: Record<string, string> = {};
      for (const [k, v] of Object.entries(message.headers || {})) {
        if (v != null) carrier[k] = Buffer.isBuffer(v) ? v.toString('utf8') : String(v);
      }
      const producerCtx = propagation.extract(ROOT_CONTEXT, carrier);
      const linkedSpanCtx = trace.getSpan(producerCtx)?.spanContext();

      const span = tracer.startSpan(
        `Consume Topic ${topic}`,
        {
          kind: SpanKind.CONSUMER,
          attributes: {
            ...messagingAttrs(topic),
            'messaging.operation': 'process',
            'messaging.kafka.partition': partition,
            ...(opts.groupId ? { 'messaging.consumer.group': opts.groupId } : {}),
          },
          links: linkedSpanCtx ? [{ context: linkedSpanCtx }] : [],
        },
        ROOT_CONTEXT, // top-level: its own trace root
      );

      try {
        await context.with(trace.setSpan(ROOT_CONTEXT, span), () =>
          opts.eachMessage(payload),
        );
      } catch (e: any) {
        span.recordException(e);
        span.setStatus({ code: SpanStatusCode.ERROR, message: String(e?.message || e) });
        throw e;
      } finally {
        span.end();
      }
    },
  });
}
