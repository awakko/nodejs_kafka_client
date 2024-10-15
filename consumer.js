import { createRequire } from 'module';
const require = createRequire(import.meta.url);

import fs from 'fs';
import { Kafka } from 'kafkajs';
const { CompressionTypes, CompressionCodecs } = require('kafkajs')
const SnappyCodec = require('kafkajs-snappy')

import dotenv from 'dotenv';
dotenv.config();


const topicName = process.env['KAFKA_TOPIC'];
CompressionCodecs[CompressionTypes.Snappy] = SnappyCodec
const kafka = new Kafka({
    clientId: 'nodeJS-app',
    brokers: [process.env['KAFKA_BROKER']],
    ssl: true,
    sasl: {
      mechanism: 'plain',
      username: process.env['KAFKA_SASL_USERNAME'],
      password: process.env['KAFKA_SASL_PASSWORD']
    },
    connectionTimeout: 60000,
  })
kafka.CompressionCodecs=CompressionCodecs;

/* ================== Consumer Configuration ============= */
const consumer = kafka.consumer({
  groupId: process.env['KAFKA_GROUP'],
  sessionTimeout: 60000,
  heartbeatInterval: 10000,
 
  // NEGATIVE_CASE INFINITE REBALANCE
  // sessionTimeout: 20000,
  // heartbeatInterval: 19000,


  retry: {
    retries: 5,  // jumlah retry sebelum dianggap gagal
    initialRetryTime: 300,  // waktu retry pertama dalam milidetik
    factor: 0.2,  // faktor kenaikan waktu retry
    multiplier: 2,  // pengali waktu retry
  },
 

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
await consumer.subscribe({ topic: topicName, fromBeginning: false })
await consumer.run({
    eachMessage: async ({ topic, partition, message}) => {
      console.log(`Received message:`, {
        value: message.value.toString(),
        headers: message.headers,
        topic: topic,
        partition: partition,
        offset: message.offset,
      })
      // await sleep(3000);
    },
})