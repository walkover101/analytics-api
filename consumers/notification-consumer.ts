import rabbitmqService, { Connection, Channel } from '../database/rabbitmq-service';
import logger from "../logger/logger";
import { db } from "../firebase";
import { Storage } from '@google-cloud/storage';
import { DOWNLOAD_STATUS } from '../models/download.model';
import axios from 'axios';

type Type = 'channel' | 'email';

type Message = {
    type: Type,
    data: SpaceChannel | Email
    retry?: number
}
type SpaceChannel = {
    channelId: string,
    message: string
}
type Email = {
    from: string,
    to: [string],
    cc?: [string],
    subject: string,
    body: string
}


const QUEUE_NAME = process.env.RABBIT_NOTIFICATION_QUEUE_NAME;
const AUTH_KEY = process.env.CHANNEL_AUTH_KEY;
const SG_API_KEY = process.env.SENDGRID_API_KEY;
const ORG_ID = process.env.CHANNEL_ORG_ID;

async function consume(connection: Connection, options: any) {
    try {
        const { queueName } = options;
        logger.info(`[CONSUMER](Notification) Creating channel...`);
        let channel = await connection.createChannel();
        channel.assertQueue(queueName, { durable: true });
        channel.prefetch(1);
        channel.consume(queueName, async (msg: any) => {
            let msgContent = msg.content.toString();
            logger.info(`[CONSUMER](Notification) New message received, started processing...`);
            let message: Message;
            let retryCount: number;
            try {
                let retryMessage = null;
                message = JSON.parse(msgContent) as Message;
                logger.info(msgContent);
                retryCount = Number(message.retry) || 0;
                await processMessage(message, options).catch(async (error) => {
                    logger.error(error);
                    retryMessage = Object.assign(message, { retry: ++retryCount, error });
                    if (retryCount <= 2) {
                        channel.sendToQueue(queueName, Buffer.from(JSON.stringify(retryMessage)), { persistent: true });
                    } else {
                        // Add message to deal letter queue
                        let deadQueueName = `dead-${queueName}`;
                        channel.assertQueue(deadQueueName)
                        channel.sendToQueue(deadQueueName, Buffer.from(JSON.stringify(retryMessage)), { persistent: true });
                    }
                });
            } catch (error) {
                logger.info(msgContent);
                logger.error(error);
            }
            channel.ack(msg);
            logger.info("Message Processed!");
        }, { noAck: false }); // Auto Ack Off
    } catch (error: any) {
        logger.error(error);
        consume(connection, options);
    }
}

async function processMessage(msg: Message, { orgId, authKey, sgApiKey }: any) {
    let notificationType = msg.type;
    switch (notificationType) {
        case 'channel':
            return await sendToChannel(msg.data as SpaceChannel, { orgId, authKey });
        case 'email':
            if (!SG_API_KEY) throw new Error("SENDGRID_API_KEY is required");
            return await sendEmail(msg.data as Email, sgApiKey);
        default:
            throw new Error("This message can't be processed");
            break;
    }
}

async function sendToChannel(msg: SpaceChannel, options: { orgId: string, authKey: string }) {
    const { orgId, authKey } = options;
    const { channelId, message } = msg;
    if (!orgId || !authKey) throw new Error("orgId or authKey is missing");
    if (!channelId) throw new Error("channelId is required");
    if (!message) throw new Error("Empty message is not allowed");


    let data = JSON.stringify({
        "content": message,
        "teamId": channelId,
        "orgId": orgId
    });
    let config = {
        method: 'post',
        url: 'https://api.intospace.io/chat/message',
        headers: {
            'authkey': authKey,
            'Content-Type': 'application/json'
        },
        data
    };
    await axios(config).catch(error => {
        logger.error(error);
    });
}

async function sendEmail(msg: Email, apiKey: string) {
    // TODO: ANKIT : Add validation
    let data = JSON.stringify({
        "to": msg.to,
        "from": msg.from,
        "subject": msg.subject,
        "html": msg.body
    });
    let config = {
        method: 'post',
        url: 'https://api.sendgrid.com/v3/mail/send',
        headers: {
            'Authorization': `Bearer ${apiKey}`,
            'Content-Type': 'application/json'
        },
        data
    };
    await axios(config).catch(error => {
        logger.error(error);
    });
}

const notificationConsumer = () => {
    logger.info(`[CONSUMER](Notification) Initiated...`);

    rabbitmqService().on("connect", (connection) => {
        connection = connection;

        if (!AUTH_KEY) {
            throw new Error("CHANNEL_AUTH_KEY not found in .env");
        }
        if (!ORG_ID) {
            throw new Error("CHANNEL_ORG_ID not found in .env");
        }
        if (!SG_API_KEY) {
            throw new Error("SENDGRID_API_KEY not found in .env");
        }

        if (!QUEUE_NAME) {
            throw new Error("RABBIT_NOTIFICATION_QUEUE_NAME not found in .env");
        }
        const options = {
            queueName: QUEUE_NAME,
            sgApiKey: SG_API_KEY,
            orgId: ORG_ID,
            authKey: AUTH_KEY
        }
        consume(connection, options);
    });
}

export {
    notificationConsumer
};