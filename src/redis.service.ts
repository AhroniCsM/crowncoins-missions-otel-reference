import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import Redis from 'ioredis';

/**
 * Thin ioredis wrapper. ioredis calls are auto-instrumented by the OTel
 * Node auto-injection, so hgetall/get/set/hset show up as Redis spans
 * automatically (matching the Sunflower "Redis hgetall/get" operations).
 */
@Injectable()
export class RedisService implements OnModuleInit, OnModuleDestroy {
  private client: Redis;

  onModuleInit() {
    const url = process.env.REDIS_URL || 'redis://redis:6379';
    this.client = new Redis(url, {
      lazyConnect: false,
      maxRetriesPerRequest: 2,
      retryStrategy: (times) => Math.min(times * 200, 2000),
    });
    this.client.on('error', (e) =>
      // eslint-disable-next-line no-console
      console.error(JSON.stringify({ level: 'WARN', component: 'redis', message: String(e?.message || e) })),
    );
  }

  async onModuleDestroy() {
    try {
      await this.client?.quit();
    } catch {
      /* ignore */
    }
  }

  async getUserMissions(userId: string): Promise<Record<string, string>> {
    const key = `missions:user:${userId}`;
    const state = await this.client.hgetall(key);
    if (!state || Object.keys(state).length === 0) {
      // seed a default mission set so reads always return something
      await this.client.hset(key, {
        daily_login: 'in_progress',
        play_3_games: 'in_progress',
        collect_coins: 'not_started',
      });
      await this.client.expire(key, 3600);
      return this.client.hgetall(key);
    }
    return state;
  }

  async getProgress(userId: string): Promise<string | null> {
    return this.client.get(`missions:progress:${userId}`);
  }

  async applyProgress(userId: string, mission: string, value: number): Promise<void> {
    const key = `missions:user:${userId}`;
    await this.client.hset(key, mission, value >= 100 ? 'completed' : 'in_progress');
    await this.client.set(`missions:progress:${userId}`, String(value), 'EX', 3600);
    await this.client.incr('missions:progress:processed:total');
  }
}
