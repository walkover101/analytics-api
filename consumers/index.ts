import "../startup/dotenv";
import '../startup/string.extensions';
import logger from "./../logger/logger";
import rabbitmqService, { Connection, Channel } from '../database/rabbitmq-service';
import { has } from 'lodash';
import { IConsumer } from "./consumer";
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
    private bufferSize: number = 1;
    private rabbitService;
    constructor(obj: IConsumer) {
        this.queue = obj.queue;
        this.processor = obj.processor;
        this.bufferSize = obj.prefetch;
        this.rabbitService = rabbitmqService();
        this.setup();
    }
    private setup() {
        this.rabbitService.on("connect", async (connection) => {
            this.connection = connection;
            this.channel = await this.connection?.createChannel();
            this.channel?.prefetch(this.bufferSize);
            this.channel?.assertQueue(this.queue, { durable: true });
            this.start();
        }).on("error", (error) => {
            logger.error(error);
        })
    }
    private start() {
        this.channel?.consume(this.queue, async (message: any) => {
            try {
                await this.processor(message, this.channel);
            } catch (error) {
                logger.error(error);
                throw error;
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



