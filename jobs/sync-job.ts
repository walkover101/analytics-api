import logger from "../logger/logger";
import firebaseLogger from '../logger/firebase-logger';
import { MongoClient, Sort } from 'mongodb';
import mongoService from '../database/mongo-service';
import { delay } from "../services/utility-service";
import { DateTime } from 'luxon';
import ReportData from '../models/report-data.model';
import RequestData from '../models/request-data.model';
import Tracker, { jobType } from "../models/trackers.model";
import OtpModel from "../models/otp-model";

let mongoConnection: MongoClient;
const COLLECTION = {
    requestData: process.env.REQUEST_DATA_COLLECTION || '',
    rtRequestData: process.env.REQUEST_DATA_COLLECTION || '',
    reportData: process.env.REPORT_DATA_COLLECTION || '',
    rtReportData: process.env.REPORT_DATA_COLLECTION || '',
    otpReport: process.env.OTP_REPORT_COLLECTION || '',
    rtOTPReport: process.env.OTP_REPORT_COLLECTION || ''
}
const FILTER_BY = {
    requestData: 'requestDate',
    reportData: 'sentTime',
    otpReport: 'requestDate',
    rtRequestData: 'rtRequestData',
    rtReportData: 'rtReportData',
    rtOTPReport: 'rtOTPReport'
}

const MONGO_DOCS_LIMIT = +(process.env.MONGO_DOCS_LIMIT || 10000);
const BATCH_SIZE = +(process.env.BATCH_SIZE || 1000);
const BUFFER_INTERVAL = +(process.env.BUFFER_INTERVAL || 5); // in mins
const LAG = +(process.env.SYNC_LAG || 48 * 60);  // Minutes | Default: 48hrs
const RETRY_INTERVAL = +(process.env.RETRY_INTERVAL || 10) * 1000; // in secs

const maxEndTime = () => DateTime.now().minus({ minutes: LAG });
const isTimeLimitExhausted = (timestamp: DateTime) => maxEndTime().diff(timestamp, 'minute').minutes <= 0;

async function initSynching(job: jobType) {
    logger.info(`MONGO_DOCS_LIMIT: ${MONGO_DOCS_LIMIT} | BATCH_SIZE: ${BATCH_SIZE} | BUFFER_INTERVAL: ${BUFFER_INTERVAL}mins | LAG: ${LAG}min`);

    while (true) {
        try {
            const tracker: any = await Tracker.findByPk(job);
            const { fromTimestamp, toTimestamp } = getTimestamps(tracker);

            if (isTimeLimitExhausted(toTimestamp)) {
                logger.info(`Time limit exhausted, going to wait for ${(BUFFER_INTERVAL + 1)}mins`);
                await delay((BUFFER_INTERVAL + 1) * 60 * 1000);
            }

            const mongoDocs = await fetchDocsFromMongo(job, fromTimestamp, toTimestamp);
            const docsToSync = skipRecordsUntilId(mongoDocs, tracker.lastDocumentId);
            if (!docsToSync.length && await upsertTracker(job, toTimestamp.toISO())) continue;
            await syncDataToBigQuery(job, docsToSync);
        } catch (error) {
            logger.error(error);
            await delay(RETRY_INTERVAL);
        }
    }
}

async function fetchDocsFromMongo(job: jobType, fromTimestamp: DateTime, toTimestamp: DateTime): Promise<any[]> {
    try {
        const sortBy: Sort = { [FILTER_BY[job]]: 1 };
        const query = {
            [FILTER_BY[job]]: {
                $gte: fromTimestamp,
                $lte: toTimestamp
            }
        };

        logger.info(`[MONGO] Fetching docs...`);
        logger.info(`[MONGO] Query - ${JSON.stringify(query)}`);
        const collection = mongoConnection.db().collection(COLLECTION[job]);
        return collection.find(query).sort(sortBy).limit(MONGO_DOCS_LIMIT).toArray();
    } catch (err: any) {
        logger.error(err);
        process.exit(1);
    }
}

async function syncDataToBigQuery(job: jobType, mongoDocs: any[]) {
    logger.info(`[BATCH PROCESSING](Total Records: ${mongoDocs.length}) Initiating...`);

    for (let i = 0; i < mongoDocs.length; i += BATCH_SIZE) {
        const batch = mongoDocs.slice(i, i + BATCH_SIZE);
        logger.info(`[BATCH PROCESSING] ${i}-${(i + BATCH_SIZE) - 1} records synching to big query...`);
        await insertBatchInBigQuery(job, batch);
        logger.info('[BATCH PROCESSING] Batch synched.');
        const lastDocument = batch.pop();
        const lastTimestamp = lastDocument && new Date(lastDocument[FILTER_BY[job]]).toISOString();
        const lastDocumentId = lastDocument?._id?.toString();
        await upsertTracker(job, lastTimestamp, lastDocumentId);
    }
}

async function insertBatchInBigQuery(job: jobType, batch: any[]) {
    try {
        if (job === jobType.REQUEST_DATA) await RequestData.insertMany(batch.map(doc => new RequestData(doc)));
        if (job === jobType.REPORT_DATA) await ReportData.insertMany(await Promise.all(batch.map(async doc => await ReportData.createAsync(doc))));
        if (job === jobType.OTP_REPORT) await OtpModel.insertMany(await Promise.all(batch.map(async doc => await OtpModel.createAsync(doc))));
    } catch (err: any) {
        if (err.name !== 'PartialFailureError') throw err;
        logger.error(`[JOB](insertBatchInBigQuery) PartialFailureError`);
        logger.error(JSON.stringify(err.errors));
        firebaseLogger.log(err.errors, `${job}SyncErrors`);
    }
}

function getTimestamps(tracker: any) {
    if (!tracker?.lastTimestamp) {
        logger.error('lastTimestamp is required.');
        process.exit(1);
    }

    const fromTimestamp = DateTime.fromJSDate(tracker.lastTimestamp);
    const toTimestamp = fromTimestamp.plus({ minutes: BUFFER_INTERVAL });
    return { fromTimestamp, toTimestamp };
}

function skipRecordsUntilId(mongoDocs: any[], documentId: string) {
    if (!documentId) return mongoDocs;
    const result = [];
    let offsetReached = false;

    for (let i = 0; i < mongoDocs.length; i++) {
        const doc = mongoDocs[i];
        if (offsetReached && result.push(doc)) continue;
        if (doc?._id == documentId) offsetReached = true;
    }

    return result;
}

function upsertTracker(job: jobType, isoTimestamp: string, lastDocumentId: string | null = null) {
    logger.info(`[UPDATE TRACKERS] Updating lastTimestamp: ${isoTimestamp}...`);
    logger.info(`[UPDATE TRACKERS] Updating lastDocumentId: ${lastDocumentId}...`);

    return Tracker.upsert({ jobType: job, lastTimestamp: isoTimestamp, lastDocumentId }).catch(err => {
        logger.error(err);
        process.exit(1);
    });
}

function initTrackers(job: jobType, lastTimestamp: string, forceReplace: boolean) {
    if (!lastTimestamp) return;
    logger.info(`[UPDATE TRACKERS] Updating lastTimestamp to ${lastTimestamp}...`);
    if (forceReplace) return upsertTracker(job, lastTimestamp);
    return Tracker.create({ jobType: job, lastTimestamp });
}

async function start(job: jobType, args: any) {
    try {
        logger.info(`[JOB](${job}SyncJob) Initiated...`);
        await initTrackers(job, args.lts, args.f);

        mongoService().on("connect", (connection: MongoClient) => {
            mongoConnection = connection;
            initSynching(job);
        });
    } catch (err: any) {
        if (err.message === 'Validation error') logger.error('lastTimestamp already exists. Use -f to force replace the current value');
        else logger.error(err);
    }
}

const requestDataSyncJob = (args: any) => start(jobType.REQUEST_DATA, args);
const reportDataSyncJob = (args: any) => start(jobType.REPORT_DATA, args);
const otpReportSyncJob = (args: any) => start(jobType.OTP_REPORT, args);

export {
    requestDataSyncJob,
    reportDataSyncJob,
    otpReportSyncJob
};
