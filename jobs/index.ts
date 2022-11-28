import "../startup/dotenv";
import '../startup/string.extensions';
import logger from "./../logger/logger";
import { has } from 'lodash';
import { requestDataSyncJob, reportDataSyncJob, otpReportSyncJob } from './sync-job';
import { dataIntegrity } from "./data-integrity";
import { requestSync } from "./request-sync";
import { reportSync } from "./report-sync";
// import { rtRequestSync } from "./rt-request-sync";
import { rtRequestSync, rtReportSync } from "./rt-sync-job";
import sequelize from '../database/sequelize-service';
import cron from 'node-cron';
const args = require('minimist')(process.argv.slice(2));

// Register your jobs here
const Jobs: any = {
    requestDataSyncJob,
    reportDataSyncJob,
    otpReportSyncJob,
    requestSync,
    reportSync,
    rtRequestSync,
    rtReportSync
};

// Register your cron jobs
const CronJobs: any = {
    dataIntegrity
};


async function main() {
    let jobs = Jobs;
    await sequelize();
    const jobName: string = args['name'] || args['n'];
    const jobFrequency: string = args['frequency'] || args['freq'] || args['f'];
    if (jobFrequency) {
        if (!cron.validate(jobFrequency)) throw new Error("Please enter a valid frequency in quotes i.e '* * * * *'")
        jobs = CronJobs;
    }
    if (isValidName(jobs, jobName)) throw new Error(`Valid job name is required\n\nAvailable jobs: \n${getAvailableJobs(jobs)}`);

    // Run/Schedule the job
    if (jobFrequency) {
        const task = cron.schedule(jobFrequency, Jobs[jobName], { scheduled: false });
        task.start();
    } else {
        jobs[jobName](args);
    }
}




function isValidName(jobs: any, jobName: string): Boolean {
    return !has(jobs, jobName)
}

function getAvailableJobs(jobs: any) {
    const jobNames = Object.keys(jobs);
    const numberedJobNames = jobNames.map((job, idx) => `${idx + 1}. ${job} -- --lts={LAST_TIMESTAMP}`);
    return numberedJobNames.join('\n');
}
main();
