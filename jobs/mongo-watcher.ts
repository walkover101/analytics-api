import logger from "../logger/logger";
import { ChangeStream, MongoClient, ObjectId, Timestamp } from 'mongodb';
import mongoService from '../database/mongo-service';
import { delay } from "../services/utility-service";
import { DateTime } from 'luxon';
import Tracker, { jobType } from "../models/trackers.model";
import ReportData from '../models/report-data.model';
import RequestData from '../models/request-data.model';
import firebaseLogger from '../logger/firebase-logger';
import OtpModel from "../models/otp-model";

const REQUEST_DATA_COLLECTION = process.env.REQUEST_DATA_COLLECTION || '';
let mongoConnection: MongoClient;
let changeStream: ChangeStream;

async function initSynching(job: jobType) {
    try {
        initMongoWatcher(job);
    } catch (error) {
        logger.error(error);
        await changeStream.close();
        await delay(10000);
        initSynching(job);
    }
}

function initMongoWatcher(job: jobType) {
    try {
        if (job === jobType.REQUEST_DATA) initRequestDataWatcher();
    } catch (err: any) {
        logger.error(err);
        process.exit(1);
    }
}

async function initRequestDataWatcher() {
    const collection = mongoConnection.db().collection(REQUEST_DATA_COLLECTION);
    logger.info(`[MONGO] Init watcher...`);

    // open a Change Stream on the "haikus" collection
    changeStream = collection.watch([], {
        fullDocument: 'updateLookup',
        batchSize: 2
    });

    while (!changeStream.closed) {
        if (await changeStream.hasNext()) {
            const next = await changeStream.next();
            console.log("Changed detected: \t", next);
            // Write your logic here
            await delay(5000)
        }
    }
}

async function start(job: jobType, args: any) {
    try {
        logger.info(`[WATCHER](${job}Watcher) Initiated...`);

        mongoService().on("connect", (connection: MongoClient) => {
            mongoConnection = connection;
            initSynching(job);
        });
    } catch (err: any) {
        logger.error(err);
    }
}

const requestDataWatcher = (args: any) => start(jobType.REQUEST_DATA, args);

export {
    requestDataWatcher
};
