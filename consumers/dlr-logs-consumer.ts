import rabbitmqService, { Connection, Channel } from '../services/rabbitmq-service';
import logger from "../logger/logger";
import { delay } from '../services/utility-service';
import DlrLog from '../models/dlr-log.model';
import dlrLogsService from "../services/dlr-logs-service";

const BUFFER_SIZE = parseInt(process.env.RABBIT_DLR_LOGS_BUFFER_SIZE || '50');
const QUEUE_NAME = process.env.RABBIT_DLR_LOGS_QUEUE_NAME || 'dlr-log';
let rabbitConnection: Connection;
let rabbitChannel: Channel;

async function start() {
    try {
        logger.info(`[CONSUMER](DLR Logs) Creating channel...`);
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
    logger.info(`[CONSUMER](DLR Logs) Buffer is empty, waiting for messages...`);

    rabbitChannel.consume(QUEUE_NAME, async (msg: any) => {
        logger.info(`[CONSUMER](DLR Logs) New message received, pushing to buffer...`);
        buffer.push(JSON.parse(msg.content.toString()));

        if (buffer.length === BUFFER_SIZE) {
            await processMsgs(buffer);
            logger.info(`[CONSUMER](DLR Logs) Messages processed, send ackowledgement...`);
            rabbitChannel.ack(msg, true); // true: Multiple Ack
            buffer = [];
            logger.info(`[CONSUMER](DLR Logs) Buffer is empty, waiting for messages...`);
        }
    }, { noAck: false }); // Auto Ack Off
}

async function processMsgs(msgs: any[]) {
    logger.info(`[CONSUMER](DLR Logs) Buffer full, processing ${msgs.length} messages...`);

    try {
        const dlrLogs: Array<DlrLog> = [];
        msgs.map(msg => msg.map((dlrlog: any) => dlrLogs.push(new DlrLog(dlrlog))));
        await dlrLogsService.insertMany(dlrLogs);
    } catch (err: any) {
        if (err.name !== 'PartialFailureError') throw err;
        logger.error(`[CONSUMER](DLR Logs) PartialFailureError`);
        logger.error(JSON.stringify(err));
    }
}

const dlrLogsConsumer = () => {
    logger.info(`[CONSUMER](DLR Logs) Initiated...`);

    rabbitmqService().on("connect", (connection) => {
        rabbitConnection = connection;
        start();
    });
}

export {
    dlrLogsConsumer
};