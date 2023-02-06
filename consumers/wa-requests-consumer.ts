import rabbitmqService, { Connection, Channel } from '../database/rabbitmq-service';
import logger from "../logger/logger";
import WARequest from '../models/wa-request.model';
import { IConsumer } from './consumer';

const BUFFER_SIZE = parseInt(process.env.RABBIT_WA_REQ_BUFFER_SIZE || '5');
const QUEUE_NAME = process.env.RABBIT_WA_REQ_QUEUE_NAME || 'wa-requests';

let batch: Array<WARequest> = [];
let bufferLength: number = 0;
async function processMsg(message: any, channel: Channel) {

    let event = message?.content;
    event = JSON.parse(event.toString());
    if (Array.isArray(event)) {
        event.forEach(e => batch.push(new WARequest(e)));
    }
    bufferLength++;
    if (bufferLength >= BUFFER_SIZE) {
        await WARequest.insertMany(batch).catch(error => {
            if (error?.name !== 'PartialFailureError') throw error;
            logger.error(`[CONSUMER](Voice Reports) PartialFailureError`);
            logger.error(JSON.stringify(error));
        });
        batch = [];
        bufferLength = 0;
        channel.ack(message, true);
    }

}


export const waRequest: IConsumer = {
    queue: QUEUE_NAME,
    processor: processMsg,
    prefetch: BUFFER_SIZE
}