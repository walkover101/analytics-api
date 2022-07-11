import logger from "../logger/logger";
import { resolve } from 'path'
import { existsSync, readFileSync, writeFileSync } from 'fs';
import { DateTime } from 'luxon';
import { ObjectId } from "mongodb";

const projectRoot = resolve('./');
const SNYC_JOB_CONFIG_FILE_PATH = `${projectRoot}/sync-jobs-config.json`;
enum jobType {
    REQUEST_DATA = 'requestData',
    REPORT_DATA = 'reportData'
}

function getSyncJobConfig() {
    try {
        const file = readFileSync(SNYC_JOB_CONFIG_FILE_PATH, "utf8");
        return JSON.parse(file);
    } catch (error) {
        if (!existsSync(SNYC_JOB_CONFIG_FILE_PATH)) {
            logger.error(`Couldn't find sync-job-config.json`);
        } else {
            logger.error(`Unable to read sync jobs config - ${SNYC_JOB_CONFIG_FILE_PATH}`);
            logger.error(error);
        }

        process.exit(1);
    }
}

function getLastTimestamp(job: jobType) {
    const lastTimeStamp = getSyncJobConfig()[job]?.trackers.lastTimestamp;
    const timeStamp = DateTime.fromISO(lastTimeStamp);

    if (!timeStamp.isValid) {
        logger.error(`Invalid ${job}.trackers.lastTimestamp - ${lastTimeStamp}`);
        process.exit(1);
    }

    return timeStamp;
}

function getLastDocumentId(job: jobType) {
    let lastDocumentId;

    try {
        lastDocumentId = getSyncJobConfig()[job]?.trackers.lastDocumentId || null;
    } catch (error) {
        lastDocumentId = null;
    }

    if (!lastDocumentId || !isValidObjectId(lastDocumentId)) {
        logger.error(`Please set valid ${job}.trackers.lastDocumentId - ${lastDocumentId}`);
        process.exit(1);
    }

    return lastDocumentId;
}

function updateTrackers(job: jobType, lastDocument: any) {
    try {
        const lastDocId = lastDocument?._id?.toString() || null;
        logger.info(`[UPDATE TRACKERS] Updating lastDocumentId to ${lastDocId}...`);
        const config = getSyncJobConfig();
        config[job].trackers.lastDocumentId = lastDocId;
        writeFileSync(SNYC_JOB_CONFIG_FILE_PATH, JSON.stringify(config, null, 2));
    } catch (error) {
        logger.error(error);
        process.exit(1);
    }
}

function isValidObjectId(id: string) {
    try {
        return new ObjectId(id).toString() === id;
    } catch {
        return false;
    }
}

export {
    getLastDocumentId,
    updateTrackers,
    jobType
}
