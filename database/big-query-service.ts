import { BigQuery } from "@google-cloud/bigquery";
import * as _ from "lodash";

const MSG91_DATASET_ID = process.env.MSG91_DATASET_ID || 'msg91_test';

const CREDENTIALS = {
    "private_key": process.env.PRIVATE_KEY,
    "client_email": process.env.CLIENT_EMAIL,
    "client_id": process.env.CLIENT_ID,
};

const bigQuery = new BigQuery({
    credentials: CREDENTIALS,
    projectId: process.env.GCP_PROJECT_ID
});

function getMsg91Dataset() {
    return bigQuery.dataset(MSG91_DATASET_ID);
}

async function getQueryResults(query: string) {
    const [job] = await getMsg91Dataset().createQueryJob({ query, location: process.env.DATA_SET_LOCATION });
    let [rows] = await job.getQueryResults();
    return rows;
}

export default getMsg91Dataset();
export {
    bigQuery,
    getQueryResults
}