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

// Step 2: send a message to your topic
async function send() {
  const producer = kafka.producer()
  await producer.connect()

  const messages = []
  // for (let i = 1; i <= 10; i++) {
  //   messages.push({ value: `KafkaJS Message ${10+i}` })
  // }
  messages.push({ value: `{
  "registertime": 1503928486016,
  "userid": "User_2",
  "name": "John",
  "email": "john@test.com",
  "regionid": "Region_6",
  "gender": "FEMALE"
}` })

  let resp = await producer.send({
    topic: topicName,
    messages: messages,
  })

  console.log(`Sent messages:`, resp)
  await producer.disconnect()
}

// Step 3: read messages 
send().catch(console.error)

