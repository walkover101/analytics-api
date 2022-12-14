import rabbitmqService, { Connection, Channel } from '../database/rabbitmq-service';
import logger from "../logger/logger";
import MailReport from '../models/mail-report.model';
import { IConsumer } from './consumer';

const BUFFER_SIZE = parseInt(process.env.RABBIT_MAIL_REP_BUFFER_SIZE || '50');
const QUEUE_NAME = process.env.RABBIT_MAIL_REP_QUEUE_NAME || 'email-response-logs';
let batch: Array<MailReport> = [];
let bufferLength: number = 0;
async function processMsgs(message: any, channel: Channel) {

    let event = message?.content;
    event = JSON.parse(event.toString());
    if (Array.isArray(event)) {
        event.forEach(e => batch.push(new MailReport(e)));
    }
    bufferLength++;
    if (bufferLength >= BUFFER_SIZE) {
        await MailReport.insertMany(batch).catch(error => {
            if (error?.name !== 'PartialFailureError') throw error;
            logger.error(`[CONSUMER](Voice Reports) PartialFailureError`);
            logger.error(JSON.stringify(error));
        });
        batch = [];
        bufferLength = 0;
        channel.ack(message, true);
    }

}

export const mailReports: IConsumer = {
    queue: QUEUE_NAME,
    processor: processMsgs,
    prefetch: BUFFER_SIZE
}
