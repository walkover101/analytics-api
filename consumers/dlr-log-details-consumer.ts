import rabbitmqService, { Connection, Channel } from '../services/rabbitmq-service';
import logger from "../logger/logger";
import { delay } from '../services/utility-service';
import DlrLogDetail from '../models/dlr-log-detail.model';
import dlrLogDetailsService from "../services/dlr-log-details-service";

const BUFFER_SIZE = parseInt(process.env.RABBIT_DLR_LOG_DETAILS_BUFFER_SIZE || '50');
const QUEUE_NAME = process.env.RABBIT_DLR_LOG_DETAILS_QUEUE_NAME || 'dlr-log-details';
let rabbitConnection: Connection;
let rabbitChannel: Channel;

async function start() {
    try {
        logger.info(`[CONSUMER](DLR Log Details) Creating channel...`);
        rabbitChannel = await rabbitConnection.createChannel();
        rabbitChannel.prefetch(BUFFER_SIZE);
        rabbitChannel.assertQueue(QUEUE_NAME, { durable: true });
        startConsumption();
    } catch (error) {
        logger.error(error);
    }
}

function startConsumption() {
    let buffer: any[] = [];
    logger.info(`[CONSUMER](DLR Log Details) Buffer is empty, waiting for messages...`);

    rabbitChannel.consume(QUEUE_NAME, async (msg: any) => {
        logger.info(`[CONSUMER](DLR Log Details) New message received, pushing to buffer...`);
        buffer.push(JSON.parse(msg.content.toString()));

        if (buffer.length === BUFFER_SIZE) {
            await processMsgs(buffer);
            logger.info(`[CONSUMER](DLR Log Details) Messages processed, send ackowledgement...`);
            // rabbitChannel.ack(msg, true); // true: Multiple Ack
            buffer = [];
            logger.info(`[CONSUMER](DLR Log Details) Buffer is empty, waiting for messages...`);
        }
    }, { noAck: false }); // Auto Ack Off
}

async function processMsgs(msgs: any[]) {
    logger.info(`[CONSUMER](DLR Log Details) Buffer full, processing ${msgs.length} messages...`);
    const dlrLogDetails: Array<DlrLogDetail> = msgs.map(msg => msg.map((dlrLogDetail: any) => new DlrLogDetail(dlrLogDetail)));
    dlrLogDetailsService.insertMany(dlrLogDetails);
    await delay(20 * 1000);
}

const dlrLogDetailsConsumer = () => {
    logger.info(`[CONSUMER](DLR Log Details) Initiated...`);

    rabbitmqService().on("connect", (connection) => {
        rabbitConnection = connection;
        start();
    });
}

export {
    dlrLogDetailsConsumer
};