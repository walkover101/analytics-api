import rabbitmqService, { Connection, Channel } from '../database/rabbitmq-service';
import logger from "../logger/logger";
import { db } from "../firebase";
import { Storage } from '@google-cloud/storage';
import { DOWNLOAD_STATUS } from '../models/download.model';
import axios from 'axios';
import { IConsumer } from './consumer';
const CREDENTIALS = {
    "private_key": process.env.PRIVATE_KEY,
    "client_email": process.env.CLIENT_EMAIL,
    "client_id": process.env.CLIENT_ID,
};
const storage = new Storage({
    credentials: CREDENTIALS,
    projectId: process.env.GCP_PROJECT_ID
});
const zipBucket = require('zip-bucket')(storage);
type ZipOptions = {
    fromBucket: string,
    fromPath: string,
    toBucket?: string,
    toPath?: string
}
type Message = {
    bucket: string,
    srcFolder: string, // Path to folder
    destFileName?: string, // Path to output file
    firebase?: {
        id: string,
        collection: string
    },
    email?: string,
    retry?: number
}
const QUEUE_NAME = process.env.RABBIT_ZIP_FOLDER_QUEUE_NAME || "";
const AUTH_KEY = process.env.CHANNEL_AUTH_KEY;
const CHANNEL_ID = process.env.CHANNEL_ID;
const ORG_ID = process.env.CHANNEL_ORG_ID;
const BASE_URL = process.env.GCS_BASE_URL;
const NOTIFICATION_QUEUE = process.env.RABBIT_NOTIFICATION_QUEUE_NAME || 'notification';

async function consume(msg: any, channel: Channel) {
    if (!QUEUE_NAME) {
        throw new Error("RABBIT_ZIP_QUEUE_NAME not found in .env");
    }
    if (!AUTH_KEY) {
        throw new Error("CHANNEL_AUTH_KEY not found in .env");
    }
    if (!ORG_ID) {
        throw new Error("CHANNEL_ORG_ID not found in .env");
    }
    if (!CHANNEL_ID) {
        throw new Error("CHANNEL_ID not found in .env");
    }
    if (!BASE_URL) {
        throw new Error("GCS_BASE_URL not found in .env");
    }
    const options = {
        authKey: AUTH_KEY,
        queueName: QUEUE_NAME,
        channelId: CHANNEL_ID,
        orgId: ORG_ID,
        baseURL: BASE_URL
    }
    let msgContent = msg.content.toString();
    logger.info(`[CONSUMER](Zip Files) New message received, started processing...`);
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
                logger.info(message);
                await sendNotification(msgContent, options).catch(reason => logger.error(reason));
            }
        }).then((value) => {
            // Send Email If Required
            if (message.email) {
                let url = `${options?.baseURL}/${message?.bucket}/${message?.srcFolder}/${message?.destFileName}.zip`
                let notification: any = {
                    type: "email",
                    data: {
                        from: "noreply@msg91.in",
                        to: [message.email],
                        subject: "MSG91 - Report Ready For Download",
                        body: `Your requested report is ready for download. \n${url}`
                    }
                }
                channel.sendToQueue(NOTIFICATION_QUEUE, Buffer.from(JSON.stringify(notification)), { persistent: true })
            }
        });
    } catch (error) {
        logger.info(msgContent);
        logger.error(error);
        await sendNotification(msgContent, options).catch(reason => logger.error(reason));
    }
    channel.ack(msg);
    logger.info("Message Processed!");
}

async function processMessage(msg: Message, { baseURL }: any) {
    const options: ZipOptions = {
        fromBucket: msg.bucket,
        fromPath: msg.srcFolder,
        toBucket: msg.bucket,
        toPath: `${msg.srcFolder}/${msg.destFileName}.zip`
    }
    await zipBucket(options);
    let docRef = null;
    try {
        // Update status to firebase
        if (msg.firebase) {
            docRef = db.collection(msg.firebase?.collection).doc(msg.firebase?.id);
            await docRef.update({
                status: DOWNLOAD_STATUS.SUCCESS,
                file: `${baseURL}/${options.toBucket}/${options.toPath}`
            });
        }
    } catch (error) {
        if (docRef) {
            await docRef.update({ status: DOWNLOAD_STATUS.ERROR, error: JSON.stringify(error) });
        }
    }
}

async function sendNotification(message: string, options: any) {
    const { authKey, channelId, orgId } = options;
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


export const zipFolder: IConsumer = {
    queue: QUEUE_NAME,
    processor: consume,
    prefetch: 1
}
