import logger from '../logger/logger';
import { MongoClient } from 'mongodb';
import rabbitmqProducer from "../database/rabbitmq-producer";
import { DateTime } from 'luxon';
import { getQueryResults, MSG91_DATASET_ID, MSG91_PROJECT_ID, REPORT_DATA_TABLE_ID, REQUEST_DATA_TABLE_ID } from '../database/big-query-service';
const CHANNEL_ID = process.env.CHANNEL_ID;
const NOTIFICATION_QUEUE = process.env.RABBIT_NOTIFICATION_QUEUE_NAME || 'notification';
const url = process.env.MONGO_CONNECTION_STRING || "";
const dbName = process.env.MONGO_DB_NAME;
const client = new MongoClient(url);
const db = client.db(dbName);
const integrityCheck = async () => {
    try {
        let startDate = DateTime.now().setZone("utc").plus({ days: -3 }).set({ hour: 0, minute: 0, second: 0 }).toFormat("yyyy-MM-dd HH:mm:ss z");
        let endDate = DateTime.now().setZone("utc").plus({ days: -3 }).set({ hour: 23, minute: 59, second: 59 }).toFormat("yyyy-MM-dd HH:mm:ss z");
        // Check for duplicate data in Report Data
        const { count: reportCount, unique: uniqueReportCount } = await getReportBQCount(startDate, endDate);
        if ((reportCount - uniqueReportCount) > 0) {
            const diff = reportCount - uniqueReportCount;
            let message = `Duplicate Data Found : \n
        Table : ${MSG91_PROJECT_ID}.${MSG91_DATASET_ID}.${REPORT_DATA_TABLE_ID}\n
        Date : ${startDate} - ${endDate}\n
        Diff : ${diff}
        `;
            await sendToChannel(message);
        }
        // Check for duplicate data in Request Data
        const { count: requestCount, unique: uniqueRequestCount } = await getRequestBQCount(startDate, endDate);
        if ((requestCount - uniqueRequestCount) > 0) {
            const diff = requestCount - uniqueRequestCount;
            let message = `Duplicate Data Found : \n
        Table : ${MSG91_PROJECT_ID}.${MSG91_DATASET_ID}.${REQUEST_DATA_TABLE_ID}\n
        Date : ${startDate} - ${endDate}\n
        Diff : ${diff}
        `;
            await sendToChannel(message);
        }
        // Match report count between Mongo and BigQuery
        const mongoReportCount = await getReportMongoCount(startDate, endDate);
        let reportDiff = mongoReportCount - uniqueReportCount;
        if (reportDiff != 0) {
            let message = `Data Missing : \n
        Table : ${MSG91_PROJECT_ID}.${MSG91_DATASET_ID}.${REPORT_DATA_TABLE_ID}\n
        Date : ${startDate} - ${endDate}\n
        Diff : ${reportDiff}
        `;
            await sendToChannel(message);
        }
        // Match request count between Mongo and BigQuery
        const mongoRequestCount = await getRequestMongoCount(startDate, endDate);
        let requestDiff = mongoRequestCount - uniqueRequestCount;
        if (requestDiff != 0) {
            let message = `Data Missing : \n
        Table : ${MSG91_PROJECT_ID}.${MSG91_DATASET_ID}.${REQUEST_DATA_TABLE_ID}\n
        Date : ${startDate} - ${endDate}\n
        Diff : ${requestDiff}
        `;
            await sendToChannel(message);
        }

    } catch (error) {
        logger.error(error);
    }
    client.close().then(value=>{
        logger.info("Mongo Connection Closed");
    })
}

async function getReportMongoCount(startTime: string, endTime: string): Promise<number> {
    const reportQuery = {
        sentTime: {
            $gte: startTime,
            $lte: endTime
        }
    }
    const collection = db.collection(process.env.REPORT_DATA_COLLECTION || "");
    return await collection.count(reportQuery);
}

async function getRequestMongoCount(startTime: string, endTime: string): Promise<number> {
    const requestQuery = {
        requestDate: {
            $gte: startTime,
            $lte: endTime
        }
    }
    const collection = db.collection(process.env.REQUEST_DATA_COLLECTION || "");
    return await collection.count(requestQuery);
}

async function getReportBQCount(startTime: string, endTime: string): Promise<{ count: number, unique: number }> {
    const query = `SELECT COUNT(_id) AS count, COUNT(DISTINCT _id) AS unique
    FROM \`${MSG91_PROJECT_ID}.${MSG91_DATASET_ID}.${REPORT_DATA_TABLE_ID}\`
    WHERE isSingleRequest IS NULL AND sentTime >= ${startTime} AND sentTime <= ${endTime}`;
    const [data] = await getQueryResults(query);
    return data;
}
async function getRequestBQCount(startTime: string, endTime: string): Promise<{ count: number, unique: number }> {
    const query = `SELECT COUNT(_id) AS count, COUNT(DISTINCT _id) AS unique
    FROM \`${MSG91_PROJECT_ID}.${MSG91_DATASET_ID}.${REQUEST_DATA_TABLE_ID}\`
    AND requestDate >= ${startTime} AND requestDate <= ${endTime}`;
    const [data] = await getQueryResults(query);
    return data;
}

async function sendToChannel(message: string) {
    if (!CHANNEL_ID && !NOTIFICATION_QUEUE) {
        throw new Error("Set CHANNEL_ID and NOTIFICATION_QUEUE in env")
    }
    await rabbitmqProducer.publishToQueue(NOTIFICATION_QUEUE, JSON.stringify({
        type: "channel",
        data: {
            channelId: CHANNEL_ID,
            message
        }
    }))
}
export { integrityCheck as dataIntegrity };