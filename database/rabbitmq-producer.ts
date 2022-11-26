import rabbitmqService, { Connection, Channel } from '../database/rabbitmq-service';
import logger from "../logger/logger";

let rabbitConnection: Connection;
let rabbitChannel: Channel;

class RabbitMqProducer {
    private static instance: RabbitMqProducer;

    constructor() {
        logger.info(`[PRODUCER] Listening for connection...`);

        // rabbitmqService().on("connect", async (connection) => {
        //     logger.info(`[PRODUCER] Connection received...`);
        //     rabbitConnection = connection;
        //     logger.info(`[PRODUCER] Creating channel...`);
        //     rabbitChannel = await rabbitConnection.createChannel();
        // });
    }

    public static getSingletonInstance(): RabbitMqProducer {
        return RabbitMqProducer.instance ||= new RabbitMqProducer();
    }

    public async publishToQueue(queueName: string, payload: any) {
        try {
            logger.info(`[PRODUCER] Preparing payload...`);
            payload = (typeof payload === 'string') ? payload : JSON.stringify(payload);
            const payloadBuffer: Buffer = Buffer.from(payload);
            logger.info(`[PRODUCER] Asserting '${queueName}' queue...`);
            await rabbitChannel.assertQueue(queueName, { durable: true });
            logger.info(`[PRODUCER] Producing to '${queueName}' queue...`);
            rabbitChannel.sendToQueue(queueName, payloadBuffer);
        } catch (error: any) {
            console.error('[RabbitMqProducer] publishToQueue', error);
            throw error;
        }
    }
}

export default RabbitMqProducer.getSingletonInstance();
