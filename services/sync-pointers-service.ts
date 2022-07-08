import logger from "../logger/logger";
import { resolve } from 'path'
import { existsSync, readFileSync, writeFileSync } from 'fs';

const projectRoot = resolve('./');
const SNYC_JOB_POINTERS_FILE_PATH = `${projectRoot}/sync-job-pointers.json`;

function getSyncJobPointers() {
    try {
        const file = readFileSync(SNYC_JOB_POINTERS_FILE_PATH, "utf8");
        return JSON.parse(file);
    } catch (error) {
        if (!existsSync(SNYC_JOB_POINTERS_FILE_PATH)) {
            writeFileSync(SNYC_JOB_POINTERS_FILE_PATH, '{}');
            throw new Error(`Please set the initial timestamp in file ${SNYC_JOB_POINTERS_FILE_PATH}`);
        }

        logger.error(error);
        throw new Error(`Unable to read timestamp from file ${SNYC_JOB_POINTERS_FILE_PATH}`);
    }
}

function getReqDataLastTimestamp() {
    const syncJobPointers = getSyncJobPointers();
    return syncJobPointers?.requestData?.lastTimestamp;
}

export {
    getReqDataLastTimestamp
}
