import rabbitmqService, { Connection, Channel } from '../database/rabbitmq-service';
import logger from "../logger/logger";
import WARequest from '../models/wa-request.model';

const BUFFER_SIZE = parseInt(process.env.RABBIT_WA_REQ_BUFFER_SIZE || '50');
const QUEUE_NAME = process.env.RABBIT_WA_REQ_QUEUE_NAME || 'wa-requests';
let rabbitConnection: Connection;
let rabbitChannel: Channel;

async function start() {
    try {
        logger.info(`[CONSUMER](WA Requests) Creating channel...`);
        rabbitChannel = await rabbitConnection.createChannel();
        rabbitChannel.prefetch(BUFFER_SIZE);
        rabbitChannel.assertQueue(QUEUE_NAME, { durable: true });
        startConsumption();
    } catch (error: any) {
        logger.error(error);
    }
}

function startConsumption() {
    let buffer: any[] = [];
    logger.info(`[CONSUMER](WA Requests) Buffer is empty, waiting for messages...`);

    rabbitChannel.consume(QUEUE_NAME, async (msg: any) => {
        logger.info(`[CONSUMER](WA Requests) New message received, pushing to buffer...`);
        buffer.push(JSON.parse(msg.content.toString()));

        if (buffer.length === BUFFER_SIZE) {
            await processMsgs(buffer);
            logger.info(`[CONSUMER](WA Requests) Messages processed, send ackowledgement...`);
            rabbitChannel.ack(msg, true); // true: Multiple Ack
            buffer = [];
            logger.info(`[CONSUMER](WA Requests) Buffer is empty, waiting for messages...`);
        }
    }, { noAck: false }); // Auto Ack Off
}

async function processMsgs(msgs: any[]) {
    logger.info(`[CONSUMER](WA Requests) Buffer full, processing ${msgs.length} messages...`);
    try {
        const waRequests: Array<WARequest> = [];
        msgs.map(msg => msg.map((waReq: any) => waRequests.push(new WARequest(waReq))));
        if (waRequests.length) await WARequest.insertMany(waRequests);
    } catch (err: any) {
        if (err.name !== 'PartialFailureError') throw err;
        logger.error(`[CONSUMER](WA Requests) PartialFailureError`);
        logger.error(JSON.stringify(err));
    }
}

const waRequestsConsumer = () => {
    logger.info(`[CONSUMER](WA Requests) Initiated...`);

    rabbitmqService().on("connect", (connection) => {
        rabbitConnection = connection;
        start();
    });
}

export {
    waRequestsConsumer
};