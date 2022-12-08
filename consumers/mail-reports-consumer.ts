import rabbitmqService, { Connection, Channel } from '../database/rabbitmq-service';
import logger from "../logger/logger";
import MailReport from '../models/mail-report.model';
// import {mail_report} from '../models/protofiles/mail_report.proto';
let protobuf = require("protobufjs");

const BUFFER_SIZE = parseInt(process.env.RABBIT_MAIL_REP_BUFFER_SIZE || '50');
const QUEUE_NAME = process.env.RABBIT_MAIL_REP_QUEUE_NAME || 'email-response-logs';
let rabbitConnection: Connection;
let rabbitChannel: Channel;

async function start() {
    try {
        logger.info(`[CONSUMER](Mail Reports) Creating channel...`);
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
    logger.info(`[CONSUMER](Mail Reports) Buffer is empty, waiting for messages...`);

    rabbitChannel.consume(QUEUE_NAME, async (msg: any) => {
        logger.info(`[CONSUMER](Mail Reports) New message received, pushing to buffer...`);
        buffer.push(JSON.parse(msg.content.toString()));

        if (buffer.length === BUFFER_SIZE) {
            await processMsgs(buffer);
            logger.info(`[CONSUMER](Mail Reports) Messages processed, send ackowledgement...`);
            // rabbitChannel.ack(msg, true); // true: Multiple Ack
            buffer = [];
            logger.info(`[CONSUMER](Mail Reports) Buffer is empty, waiting for messages...`);
        }
    }, { noAck: false }); // Auto Ack Off
}

// async function processMsgs(msgs: any[]) {
//     logger.info(`[CONSUMER](Mail Reports) Buffer full, processing ${msgs.length} messages...`);

//     try {
//         const mailReports: Array<MailReport> = [];
//         msgs.map(msg => msg.map((mailRep: any) => mailReports.push(new MailReport(mailRep))));
//         if (mailReports.length) await MailReport.insertMany(mailReports);
//     } catch (err: any) {
//         if (err.name !== 'PartialFailureError') throw err;
//         logger.error(`[CONSUMER](Mail Reports) PartialFailureError`);
//         logger.error(JSON.stringify(err));
//     }
// }

async function processMsgs(msgs: any[]) {
    logger.info(`[CONSUMER](Mail Reports) Buffer full, processing ${msgs} messages...`);
    try {
        protobuf.load("mail_report.proto", function(err: any, root: { lookupType: (arg0: string) => any; }) {
            if (err) throw err;
            const mailReport = root.lookupType("mailReport.mail_report");
            const mailReports: Array<MailReport> = [];
            
            msgs.map(msg => msg.map(async (mailRep: any) => {
                let message = mailReport.create(new MailReport(mailRep));
                console.log(`message = ${JSON.stringify(message)}`);
                let buffer = mailReport.encode(message).finish();
                console.log(`buffer = ${Array.prototype.toString.call(buffer)}`);
                mailReports.push(buffer);
                if (mailReports.length) await MailReport.insertMany(mailReports);
            }));
        });
    } catch (err: any) {
        if (err.name !== 'PartialFailureError') throw err;
        logger.error(`[CONSUMER](Mail Reports) PartialFailureError`);
        logger.error(JSON.stringify(err));
    }

    // });
   
}

const mailReportsConsumer = (message: any) => {
    logger.info(`[CONSUMER](Mail Reports) Initiated...`);
    processMsgs(message)
    rabbitmqService().on("connect", (connection) => {
        rabbitConnection = connection;
        // start();
    });
}

export {
    mailReportsConsumer
};