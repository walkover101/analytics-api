import { Channel } from '../database/rabbitmq-service';
import logger from "../logger/logger";
import MailEvent from '../models/mail-event.model';
import { IConsumer } from './consumer';

const BUFFER_SIZE = parseInt(process.env.RABBIT_MAIL_EVENTS_BUFFER_SIZE || '50');
const QUEUE_NAME = process.env.RABBIT_MAIL_EVENTS_QUEUE_NAME || 'email-event-logs';

let batch: Array<MailEvent> = [];
let bufferLength: number = 0;
async function processMsgs(message: any, channel: Channel) {
    try {
        let event = message?.content;
        event = JSON.parse(event.toString());
        if (Array.isArray(event)) {
            event.forEach(e => batch.push(new MailEvent(e)));
        }
        bufferLength++;
        if (bufferLength >= BUFFER_SIZE) {
            await MailEvent.insertMany(batch);
            batch = [];
            bufferLength = 0;
        } else {
            return;
        }
    } catch (error: any) {
        if (error?.name !== 'PartialFailureError') throw error;
        logger.error(`[CONSUMER](Mail Requests) PartialFailureError`);
        logger.error(JSON.stringify(error));
    }
    channel.ack(message, true);
}

export const mailEvent: IConsumer = {
    queue: QUEUE_NAME,
    processor: processMsgs,
    prefetch: BUFFER_SIZE
}