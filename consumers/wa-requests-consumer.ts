import rabbitmqService, { Connection, Channel } from '../database/rabbitmq-service';
import logger from "../logger/logger";
import WARequest from '../models/wa-request.model';
import { Consumer } from './consumer';

const BUFFER_SIZE = parseInt(process.env.RABBIT_WA_REQ_BUFFER_SIZE || '5');
const QUEUE_NAME = process.env.RABBIT_WA_REQ_QUEUE_NAME || 'wa-requests';

let batch: Array<WARequest> = [];
async function processMsgs(message: any, channel: Channel) {

    try {
        let event = message?.content;
        event = JSON.parse(event.toString());
        console.log(event);
        if (Array.isArray(event)) {
            event.forEach(e => batch.push(new WARequest(e)));
        }
        if (batch.length >= BUFFER_SIZE) {
            console.log(batch);
            await WARequest.insertMany(batch);
            batch = [];
            channel.ack(message, true);
        };
    } catch (error: any) {
        if (error?.name !== 'PartialFailureError') throw error;
        logger.error(`[CONSUMER](Mail Requests) PartialFailureError`);
        logger.error(JSON.stringify(error));
    }

}

export const waRequest: Consumer = {
    queue: QUEUE_NAME,
    processor: processMsgs
}