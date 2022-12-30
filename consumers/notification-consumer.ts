import { Channel } from '../database/rabbitmq-service';
import logger from "../logger/logger";
import axios from 'axios';
import { IConsumer } from './consumer';

type Type = 'channel' | 'email' | 'slack';

type Message = {
    type: Type,
    data: SpaceChannel | Email | Slack
    retry?: number
}

type SpaceChannel = {
    channelId: string,
    message: string
}

type Slack = {
    channelName: string,
    message: string,
    iconEmoji: string,
    username: string
}

type Email = {
    from: string,
    to: [string],
    cc?: [string],
    subject: string,
    body: string
}


const QUEUE_NAME = process.env.RABBIT_NOTIFICATION_QUEUE_NAME || "";
const AUTH_KEY = process.env.CHANNEL_AUTH_KEY;
const SG_API_KEY = process.env.SENDGRID_API_KEY;
const ORG_ID = process.env.CHANNEL_ORG_ID;
const WALKOVER_SLACK_URL = process.env.WALKOVER_SLACK_URL || 'https://hooks.slack.com/services/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';
const SLACK_AUTHKEY = process.env.SLACK_AUTHKEY || 'MbpzswBCQmwwgXScf3c';


async function consume(msg: any, channel: Channel) {
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
                channel.sendToQueue(options.queueName, Buffer.from(JSON.stringify(retryMessage)), { persistent: true });
            } else {
                // Add message to deal letter queue
                let deadQueueName = `dead-${options.queueName}`;
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
}
async function processMessage(msg: Message, { orgId, authKey, sgApiKey, slackUrl = WALKOVER_SLACK_URL }: any) {
    let notificationType = msg.type;

    switch (notificationType) {
        case 'channel':
            return await sendToChannel(msg.data as SpaceChannel, { orgId, authKey });
        case 'slack':
            return await sendToSlack(msg.data as Slack, { url: slackUrl, authKey });
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
    await axios(config);
}

async function sendToSlack(slack: Slack, options: { url: string, authKey: string }) {
    const { url, authKey } = options;
    const { channelName, message, iconEmoji, username } = slack;
    if (!authKey) throw new Error("authKey is missing");
    if (!channelName) throw new Error("channelName is required");
    if (!message) throw new Error("Empty message is not allowed");

    let data = JSON.stringify({
        channel: channelName,
        username: username || 'MSG91 Reports',
        icon_emoji: iconEmoji || 'page_with_curl',
        text: message
    });

    let config = {
        method: 'post',
        url: `${url}?authkey=${authKey}`,
        data
    };

    await axios(config);
}

async function sendEmail(msg: Email, apiKey: string) {
    // TODO: ANKIT : Add validation

    let data = JSON.stringify({
        "personalizations": [{ "to": [{ "email": msg?.to[0] }] }],
        "from": { "email": msg.from },
        "subject": msg.subject,
        "content": [{
            "type": "text/plain",
            "value": msg.body
        }]
    })
    let config = {
        method: 'post',
        url: 'https://api.sendgrid.com/v3/mail/send',
        headers: {
            'Authorization': `Bearer ${apiKey}`,
            'Content-Type': 'application/json'
        },
        data
    };
    await axios(config);
}

export const notification: IConsumer = {
    queue: QUEUE_NAME,
    processor: consume
}