import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import {
  FastifyAdapter,
  NestFastifyApplication,
} from '@nestjs/platform-fastify';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create<NestFastifyApplication>(
    AppModule,
    new FastifyAdapter({ logger: false }),
  );

  // The Fastify server span is named "Fastify/GET//route" by @fastify/otel's
  // requestHook (see otel/coralogix-autoinstrumentation.js) — no app code needed.
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
