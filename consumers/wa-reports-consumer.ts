import rabbitmqService, { Connection, Channel } from '../database/rabbitmq-service';
import logger from "../logger/logger";
import WAReport from '../models/wa-report.model';
import { IConsumer } from './consumer';

const BUFFER_SIZE = parseInt(process.env.RABBIT_WA_REP_BUFFER_SIZE || '50');
const QUEUE_NAME = process.env.RABBIT_WA_REP_QUEUE_NAME || 'wa-requests';


let batch: Array<WAReport> = [];
let bufferLength: number = 0;
async function processMsg(message: any, channel: Channel) {

    let event = message?.content;
    event = JSON.parse(event.toString());
    if (Array.isArray(event)) {
        event.forEach(e => batch.push(new WAReport(e)));
    }
    bufferLength++;
    if (bufferLength >= BUFFER_SIZE) {
        await WAReport.insertMany(batch).catch(error => {
            if (error?.name !== 'PartialFailureError') throw error;
            logger.error(`[CONSUMER](Voice Reports) PartialFailureError`);
            logger.error(JSON.stringify(error));
        });
        batch = [];
        bufferLength = 0;
        channel.ack(message, true);
    }

}


export const waReport: IConsumer = {
    queue: QUEUE_NAME,
    processor: processMsg,
    prefetch: BUFFER_SIZE
}