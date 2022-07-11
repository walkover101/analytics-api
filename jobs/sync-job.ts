import logger from "../logger/logger";
import { MongoClient, ObjectId } from 'mongodb';
import mongoService from '../services/mongo-service';
import { delay } from "../services/utility-service";
import { getLastDocumentId, jobType, updateTrackers } from "../services/sync-jobs-config-service";
import { DateTime } from 'luxon';
import requestDataService from "../services/request-data-service";
import reportDataService from "../services/report-data-service";

let mongoConnection: MongoClient;
const REQUEST_DATA_COLLECTION = process.env.REQUEST_DATA_COLLECTION || '';
const REPORT_DATA_COLLECTION = process.env.REPORT_DATA_COLLECTION || '';
const DELAY_INTERVAL = 30 * 1000; // in secs
const MONGO_DOCS_LIMIT = 10000;
const BATCH_SIZE = 1000;
const LAG = 48 * 60;  // Hours * Minutes

async function initSynching(job: jobType) {
    logger.info(`BATCH_SIZE: ${BATCH_SIZE} | MONGO_DOCS_LIMIT: ${MONGO_DOCS_LIMIT} | DELAY_INTERVAL: ${DELAY_INTERVAL}`);

    while (true) {
        try {
            const mongoDocs = await fetchDocsFromMongo(job);
            await syncDataToBigQuery(job, mongoDocs);

            if (mongoDocs.length < MONGO_DOCS_LIMIT) {
                logger.info(`Insufficient records to create a batch, going to wait for ${DELAY_INTERVAL / 1000}sec`);
                await delay(DELAY_INTERVAL);
            }
        } catch (error) {
            logger.error(error);
            await delay(10000);
        }
    }
}

async function syncDataToBigQuery(job: jobType, mongoDocs: any[]) {
    logger.info(`[BATCH PROCESSING](Total Records: ${mongoDocs.length}) Initiating...`);

    for (let i = 0; i < mongoDocs.length; i += BATCH_SIZE) {
        const batch = mongoDocs.slice(i, i + BATCH_SIZE);
        logger.info(`[BATCH PROCESSING] ${i}-${(i + BATCH_SIZE) - 1} records synching to big query...`);
        await insertBatchInBigQuery(job, batch);
        logger.info('[BATCH PROCESSING] Batch synched.');
        updateTrackers(job, batch.pop());
    }
}

async function insertBatchInBigQuery(job: jobType, batch: any[]) {
    try {
        if (job === jobType.REQUEST_DATA) await requestDataService.insertMany(batch);
        if (job === jobType.REPORT_DATA) await reportDataService.insertMany(batch);
    } catch (err: any) {
        if (err.name !== 'PartialFailureError') throw err;
        logger.error(err.message);
    }
}

function fetchDocsFromMongo(job: jobType): Promise<any[]> {
    const lastDocumentId: string = getLastDocumentId(job);
    if (job === jobType.REQUEST_DATA) return fetchRequestDataDocs(maxEndTime(), lastDocumentId);
    if (job === jobType.REPORT_DATA) return fetchReportDataDocs(maxEndTime(), lastDocumentId);
    return Promise.resolve([]);
}

function fetchRequestDataDocs(maxEndTime: DateTime, lastDocumentId: string) {
    const query = {
        _id: { $gt: new ObjectId(lastDocumentId) },
        requestDate: { $lte: maxEndTime }
    }

    logger.info(`[MONGO] Fetching docs...`);
    logger.info(query);
    const collection = mongoConnection.db().collection(REQUEST_DATA_COLLECTION);
    return collection.find(query).limit(MONGO_DOCS_LIMIT).sort({ requestDate: 1 }).toArray();
}

function fetchReportDataDocs(maxEndTime: DateTime, lastDocumentId: string) {
    const query = {
        _id: { $gt: new ObjectId(lastDocumentId) },
        sentTime: { $lte: maxEndTime }
    }

    logger.info(`[MONGO] Fetching docs...`);
    logger.info(query);
    const collection = mongoConnection.db().collection(REPORT_DATA_COLLECTION);
    return collection.find(query).limit(MONGO_DOCS_LIMIT).sort({ requestDate: 1 }).toArray();
}

function maxEndTime() {
    return DateTime.now().minus({ minutes: LAG });
}

function start(job: jobType) {
    logger.info(`[JOB](${job}SyncJob) Initiated...`);

    mongoService().on("connect", (connection: MongoClient) => {
        mongoConnection = connection;
        initSynching(job);
    });
}

const requestDataSyncJob = () => {
    start(jobType.REQUEST_DATA);
}

const reportDataSyncJob = () => {
    start(jobType.REPORT_DATA);
}

export {
    requestDataSyncJob,
    reportDataSyncJob
};
