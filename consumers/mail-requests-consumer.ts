import { Channel } from "amqplib";
import logger from "../logger/logger";
import MailRequest from '../models/mail-request.model';
import { IConsumer } from "./consumer";

const BUFFER_SIZE = parseInt(process.env.RABBIT_MAIL_REQ_BUFFER_SIZE || '50');
const QUEUE_NAME = process.env.RABBIT_MAIL_REQ_QUEUE_NAME || 'email-request-logs';


let batch: Array<MailRequest> = [];
async function processMsg(message: any, channel: Channel) {
    try {
        let event = message?.content;
        event = JSON.parse(event.toString());
        if (Array.isArray(event)) {
            event.forEach(e => batch.push(new MailRequest(e)));
        }
        if (batch.length >= BUFFER_SIZE) {
            await MailRequest.insertMany(batch);
            batch = [];
            channel.ack(message, true);
        };
    } catch (error: any) {
        if (error?.name !== 'PartialFailureError') throw error;
        logger.error(`[CONSUMER](Mail Requests) PartialFailureError`);
        logger.error(JSON.stringify(error));
    }
}

export const mailRequests: IConsumer = {
    queue: QUEUE_NAME,
    processor: processMsg
}

