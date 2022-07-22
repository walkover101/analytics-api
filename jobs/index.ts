import "../startup/dotenv";
import logger from "./../logger/logger";
import { has } from 'lodash';
import { requestDataSyncJob, reportDataSyncJob } from './sync-job';
const argv = require('minimist')(process.argv.slice(2));

// Register your jobs here
const Jobs: any = {
    requestDataSyncJob,
    reportDataSyncJob
};

function invalidJobName(jobName: string): Boolean {
    return !has(Jobs, jobName)
}

function getAvailableJobs() {
    const jobNames = Object.keys(Jobs);
    const numberedJobNames = jobNames.map((job, idx) => `${idx + 1}. ${job} -- --ldi={LAST_DOCUMENT_ID}`);
    return numberedJobNames.join('\n');
}

function main() {
    const jobName = process.argv[2];

    if (invalidJobName(jobName))
        return logger.error(`Valid job name is required\n\nAvailable jobs: \n${getAvailableJobs()}`);

    Jobs[jobName](argv);
}

main();
