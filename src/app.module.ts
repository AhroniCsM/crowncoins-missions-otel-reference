import { Module } from '@nestjs/common';
import { MissionsController } from './missions.controller';
import { RedisService } from './redis.service';
import { KafkaService } from './kafka.service';
import { LoadGenService } from './loadgen.service';

@Module({
  controllers: [MissionsController],
  providers: [RedisService, KafkaService, LoadGenService],
})
export class AppModule {}
