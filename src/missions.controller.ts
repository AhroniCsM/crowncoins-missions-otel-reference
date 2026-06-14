import { Body, Controller, Get, Post, Query } from '@nestjs/common';
import { RedisService } from './redis.service';
import { KafkaService } from './kafka.service';

@Controller()
export class MissionsController {
  constructor(
    private readonly redis: RedisService,
    private readonly kafka: KafkaService,
  ) {}

  // Mirrors the real Sunflower top transaction: Fastify/GET//missions/v2/get-user-missions
  @Get('/missions/v2/get-user-missions')
  async getUserMissions(@Query('userId') userId = randomUser()) {
    const missions = await this.redis.getUserMissions(userId);
    const progress = await this.redis.getProgress(userId);
    // Emit a "rank" produce on read, mirroring cx498's busiest topic.
    await this.kafka.produce('rank', { userId, ts: Date.now() });
    return { userId, missions, progress };
  }

  // Kicks the async flow: produce a missions-progress event for the consumer.
  @Post('/missions/progress')
  async postProgress(@Body() body: any) {
    const userId = String(body?.userId || randomUser());
    const mission = String(body?.mission || pick(MISSIONS));
    const value = Number(body?.value ?? Math.floor(Math.random() * 120));
    await this.kafka.produce('missions-progress', { userId, mission, value });
    return { enqueued: true, userId, mission, value };
  }

  @Get('/healthz')
  healthz() {
    return { status: 'ok' };
  }

  @Get('/readyz')
  readyz() {
    return { status: 'ready' };
  }

  @Get('/metrics')
  metrics() {
    return '# missions-sim metrics\nup 1\n';
  }
}

const MISSIONS = ['daily_login', 'play_3_games', 'collect_coins', 'invite_friend'];
function pick<T>(arr: T[]): T {
  return arr[Math.floor(Math.random() * arr.length)];
}
function randomUser(): string {
  return `u${Math.floor(Math.random() * 5000)}`;
}
