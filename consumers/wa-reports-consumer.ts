import rabbitmqService, { Connection, Channel } from '../database/rabbitmq-service';
import logger from "../logger/logger";
import WAReport from '../models/wa-report.model';

const BUFFER_SIZE = parseInt(process.env.RABBIT_WA_REP_BUFFER_SIZE || '50');
const QUEUE_NAME = process.env.RABBIT_WA_REP_QUEUE_NAME || 'wa-requests';
let rabbitConnection: Connection;
let rabbitChannel: Channel;

async function start() {
    try {
        logger.info(`[CONSUMER](WA Reports) Creating channel...`);
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
    logger.info(`[CONSUMER](WA Reports) Buffer is empty, waiting for messages...`);

    rabbitChannel.consume(QUEUE_NAME, async (msg: any) => {
        logger.info(`[CONSUMER](WA Reports) New message received, pushing to buffer...`);
        buffer.push(JSON.parse(msg.content.toString()));

        if (buffer.length === BUFFER_SIZE) {
            await processMsgs(buffer);
            logger.info(`[CONSUMER](WA Reports) Messages processed, send ackowledgement...`);
            rabbitChannel.ack(msg, true); // true: Multiple Ack
            buffer = [];
            logger.info(`[CONSUMER](WA Reports) Buffer is empty, waiting for messages...`);
        }
    }, { noAck: false }); // Auto Ack Off
}

async function processMsgs(msgs: any[]) {
    logger.info(`[CONSUMER](WA Reports) Buffer full, processing ${msgs.length} messages...`);

    try {
        const waReports: Array<WAReport> = [];
        msgs.map(msg => msg.map((waRep: any) => waReports.push(new WAReport(waRep))));
        if (waReports.length) await WAReport.insertMany(waReports);
    } catch (err: any) {
        if (err.name !== 'PartialFailureError') throw err;
        logger.error(`[CONSUMER](WA Reports) PartialFailureError`);
        logger.error(JSON.stringify(err));
    }
}

const waReportsConsumer = () => {
    logger.info(`[CONSUMER](WA Reports) Initiated...`);

    rabbitmqService().on("connect", (connection) => {
        rabbitConnection = connection;
        start();
    });
}

export {
    waReportsConsumer
};