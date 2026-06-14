import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import {
  FastifyAdapter,
  NestFastifyApplication,
} from '@nestjs/platform-fastify';
import { trace } from '@opentelemetry/api';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create<NestFastifyApplication>(
    AppModule,
    new FastifyAdapter({ logger: false }),
  );

  // Under the Nest Fastify adapter the OTel HTTP instrumentation can't resolve the
  // route template, so the server span stays named by bare method ("GET"). Rename
  // it to the full route so Coralogix shows
  // "Fastify/GET//missions/v2/get-user-missions" (matching the real service).
  // Capture the HTTP server span at onRequest, rename once the route is known at
  // onResponse. (This works with the STOCK auto-instrumentation image; we do NOT
  // suppress the HTTP server span — suppressing it also suppresses downstream
  // spans like the Kafka producer span created in the handler.)
  const fastify = app.getHttpAdapter().getInstance();
  fastify.addHook('onRequest', (req: any, _reply: any, done: any) => {
    req.__otelHttpSpan = trace.getActiveSpan();
    done();
  });
  fastify.addHook('onResponse', (req: any, _reply: any, done: any) => {
    const span = req.__otelHttpSpan;
    const route = req.routeOptions?.url || req.routerPath;
    if (span && route) {
      span.updateName(`Fastify/${req.method}/${route}`);
      span.setAttribute('http.route', route);
    }
    done();
  });

  const port = parseInt(process.env.PORT || '5550', 10);
  await app.listen(port, '0.0.0.0');
  // eslint-disable-next-line no-console
  console.log(
    JSON.stringify({
      level: 'INFO',
      service: process.env.OTEL_SERVICE_NAME || 'crowncoins-production-missions',
      message: `missions-sim listening on :${port}`,
    }),
  );
}

bootstrap().catch((err) => {
  // eslint-disable-next-line no-console
  console.error(JSON.stringify({ level: 'FATAL', message: 'bootstrap failed', error: String(err) }));
  process.exit(1);
});
