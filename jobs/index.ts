import "../startup/dotenv";
import logger from "./../logger/logger";
import { has } from 'lodash';
import requestDataSyncJob from './request-data-sync-job';

// Register your jobs here
const Jobs: any = {
    requestDataSyncJob
};

function main() {
    const jobName = process.argv[2];

    if (has(Jobs, jobName)) Jobs[jobName]();
    else return logger.error(`Valid job name is required, below is the list of available jobs
${Object.keys(Jobs).join('\n')}`)
}

main();
