import fs from 'fs';
import { Kafka } from 'kafkajs';

import dotenv from 'dotenv';
dotenv.config();

const topicName = process.env['KAFKA_TOPIC'];
const kafka = new Kafka({
    clientId: 'nodeJS-app',
    brokers: [process.env['KAFKA_BROKER']],
    ssl: true,
    sasl: {
      mechanism: 'plain',
      username: process.env['KAFKA_SASL_USERNAME'],
      password: process.env['KAFKA_SASL_PASSWORD']
    },
  })

/* ================== Consumer Configuration ============= */
const consumer = kafka.consumer({
  groupId: process.env['KAFKA_GROUP'],
  sessionTimeout: 30000, //default
  rebalanceTimeout: 60000, //default
  heartbeatInterval: 3000, //default
  maxWaitTimeInMs: 50000,  //default

  // metadataMaxAge: <Number>,
  // allowAutoTopicCreation: <Boolean>,
  // maxBytesPerPartition: <Number>,
  // minBytes: <Number>,
  // maxBytes: <Number>,
  // retry: <Object>,
  // maxInFlightRequests: <Number>,
  // rackId: <String></String>,

  //more info = https://kafka.js.org/docs/consuming#options
})
await consumer.connect()
await consumer.subscribe({ topic: topicName, fromBeginning: true })
await consumer.run({
    eachMessage: async ({ topic, partition, message}) => {
      console.log(`Received message:`, {
        value: message.value.toString(),
        headers: message.headers,
        topic: topic,
        partition: partition,
        offset: message.offset,
      })
    },
})