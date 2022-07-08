import logger from "../logger/logger";
import { MongoClient } from 'mongodb';
import mongoService from '../services/mongo-service';
import { delay } from "../services/utility-service";
import { getReqDataLastTimestamp } from "../services/sync-pointers-service";
import { DateTime } from 'luxon';

const LAG = 48 * 60;  // Hours * Minutes
const INTERVAL = 5   // Minutes
let mongoConnection: MongoClient;

async function initSynching() {
    while (true) {
        let startTime: DateTime = getStartTime();
        let endTime: DateTime = getEndTime(startTime);
        if (endTimeReached(endTime)) await delay((INTERVAL * 1000) / 4);
        processBatch(startTime, endTime);
    }
}

function processBatch(startTime: DateTime, endTime: DateTime) {

}

function getStartTime(): DateTime {
    return DateTime.fromISO(getReqDataLastTimestamp());
}

function getEndTime(startTime: DateTime): DateTime {
    return startTime.plus({ minutes: INTERVAL });
}

function endTimeReached(endTime: DateTime): Boolean {
    const currentTime = DateTime.now();
    const maxEndTime = currentTime.minus({ minutes: LAG });
    return maxEndTime.diff(endTime, 'minute').minutes <= 0
}

function start() {
    logger.info('[JOB](RequestDataSyncJob) Initiated...');

    mongoService().on("connect", (connection: MongoClient) => {
        mongoConnection = connection;
        initSynching();
    });
}

export default start;
