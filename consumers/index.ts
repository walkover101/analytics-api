import "../startup/dotenv";
import '../startup/string.extensions';
import logger from "./../logger/logger";
import rabbitmqService, { Connection, Channel } from '../database/rabbitmq-service';
import { has } from 'lodash';
import * as consumers from './consumer';

function invalidConsumerName(consumerName: string): Boolean {
    return !has(consumers, consumerName)
}

function getAvailableConsumers() {
    const consumerNames = Object.keys(consumers);
    const numberedConsumerNames = consumerNames.map((consumer, idx) => `${idx + 1}. ${consumer}`);
    return numberedConsumerNames.join('\n');
}

class Consumer {
    private connection?: Connection;
    private channel?: Channel;
    private queue: string;
    private processor: Function;
    constructor(obj: { queue: string, processor: Function }) {
        this.queue = obj.queue;
        this.processor = obj.processor;
        this.setup();
    }
    private setup() {
        rabbitmqService().on("connect", async (connection) => {
            this.connection = connection;
            this.channel = await this.connection?.createChannel();
            this.channel?.prefetch(10);
            this.channel?.assertQueue(this.queue, { durable: true });
            this.start();
        })
    }
    private start() {
        this.channel?.consume(this.queue, async (message: any) => {
            try {
                await this.processor(message, this.channel);
                // this.channel?.ack(message);
            } catch (error) {
                logger.error(error);
                // this.channel?.nack(message);
            }
        }, { noAck: false })
    }
}

function main() {
    const consumerName: string = process.argv[2];
    if (invalidConsumerName(consumerName))
        return logger.error(`Valid consumer name is required\n\nAvailable consumers: \n${getAvailableConsumers()}`);
    new Consumer((consumers as any)[consumerName]);
}

main();



