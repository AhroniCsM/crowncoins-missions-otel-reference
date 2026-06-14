import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';

/**
 * Built-in load generator: continuously drives the app's own HTTP endpoints so
 * traces ALWAYS flow into Coralogix with zero external traffic. Hitting the
 * real HTTP port (not internal calls) produces Fastify server spans + outgoing
 * http client spans, and the endpoints in turn drive Redis + Kafka spans.
 */
@Injectable()
export class LoadGenService implements OnModuleInit, OnModuleDestroy {
  private timer: NodeJS.Timeout;
  private readonly base = `http://127.0.0.1:${process.env.PORT || '5550'}`;

  onModuleInit() {
    const rps = Math.max(1, parseInt(process.env.LOAD_RPS || '5', 10));
    const intervalMs = Math.floor(1000 / rps);
    // small delay so the HTTP server is fully listening first
    setTimeout(() => {
      this.timer = setInterval(() => void this.tick(), intervalMs);
    }, 4000);
  }

  private async tick() {
    try {
      const roll = Math.random();
      if (roll < 0.6) {
        await this.get(`/missions/v2/get-user-missions?userId=u${rand(5000)}`);
      } else if (roll < 0.9) {
        await this.post('/missions/progress', {
          userId: `u${rand(5000)}`,
          mission: 'play_3_games',
          value: rand(120),
        });
      } else {
        await this.get('/healthz');
      }
    } catch {
      /* ignore transient errors; loop must never die */
    }
  }

  // The load generator's own outgoing calls would show up as client-span noise.
  // We disable the undici instrumentation for this service
  // (OTEL_NODE_DISABLED_INSTRUMENTATIONS=undici) so only the server spans remain.
  private async get(path: string) {
    await fetch(`${this.base}${path}`).then((r) => r.text());
  }

  private async post(path: string, body: unknown) {
    await fetch(`${this.base}${path}`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(body),
    }).then((r) => r.text());
  }

  onModuleDestroy() {
    if (this.timer) clearInterval(this.timer);
  }
}

function rand(n: number): number {
  return Math.floor(Math.random() * n);
}
