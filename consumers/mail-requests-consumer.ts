import { Channel } from "amqplib";
import logger from "../logger/logger";
import MailRequest from '../models/mail-request.model';
import { IConsumer } from "./consumer";

const BUFFER_SIZE = parseInt(process.env.RABBIT_MAIL_REQ_BUFFER_SIZE || '50');
const QUEUE_NAME = process.env.RABBIT_MAIL_REQ_QUEUE_NAME || 'email-request-logs';


let batch: Array<MailRequest> = [];
let bufferLength: number = 0;
async function processMsg(message: any, channel: Channel) {
    let event = message?.content;
    event = JSON.parse(event.toString());
    if (Array.isArray(event)) {
        event.forEach(e => batch.push(new MailRequest(e)));
    }
    bufferLength++;
    if (bufferLength >= BUFFER_SIZE) {
        await MailRequest.insertMany(batch).catch(error => {
            if (error?.name !== 'PartialFailureError') throw error;
            logger.error(`[CONSUMER](Voice Reports) PartialFailureError`);
            logger.error(JSON.stringify(error));
        });
        batch = [];
        bufferLength = 0;
        channel.ack(message, true);
    }
}

export const mailRequests: IConsumer = {
    queue: QUEUE_NAME,
    processor: processMsg,
    prefetch: BUFFER_SIZE
}

