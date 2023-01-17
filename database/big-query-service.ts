import { BigQuery } from "@google-cloud/bigquery";
import * as _ from "lodash";
const { BigQueryWriteClient } = require('@google-cloud/bigquery-storage').v1;

export const MSG91_DATASET_ID = process.env.MSG91_DATASET_ID || 'msg91_test';
export const MSG91_PROJECT_ID = process.env.GCP_PROJECT_ID || "msg91-reports";
export const MSG91_DATA_SET_LOCATION = process.env.DATA_SET_LOCATION || "asia-south1";
export const REQUEST_DATA_TABLE_ID = process.env.REQUEST_DATA_TABLE_ID || 'request_data';
export const REPORT_DATA_TABLE_ID = process.env.REPORT_DATA_TABLE_ID || 'report_data';
export const OTP_TABLE_ID = process.env.OTP_TABLE_ID || 'otp_report';
export const MAIL_REQ_TABLE_ID = process.env.MAIL_REQ_TABLE_ID || 'mail_request';
export const MAIL_REP_TABLE_ID = process.env.MAIL_REP_TABLE_ID || 'mail_report';
export const MAIL_EVENTS_TABLE_ID = process.env.MAIL_EVENTS_TABLE_ID || 'mail_event';
export const WA_REQ_TABLE_ID = process.env.WA_REQ_TABLE_ID || 'wa_request';
export const WA_REP_TABLE_ID = process.env.WA_REP_TABLE_ID || 'wa_report';
export const VOICE_REP_TABLE_ID = process.env.VOICE_REP_TABLE_ID || 'voice_report';
export const VOICE_REQ_TABLE_ID = process.env.VOICE_REQ_TABLE_ID || 'voice_request';
export const mode = require('@google-cloud/bigquery-storage').protos.google.cloud.bigquery.storage.v1.WriteStream.Type;
export const type = require('@google-cloud/bigquery-storage').protos.google.protobuf.FieldDescriptorProto.Type;

const CREDENTIALS = {
    "private_key": process.env.PRIVATE_KEY,
    "client_email": process.env.CLIENT_EMAIL,
    "client_id": process.env.CLIENT_ID,
};

const bigQuery = new BigQuery({
    credentials: CREDENTIALS,
    projectId: process.env.GCP_PROJECT_ID
});

const writeClient = new BigQueryWriteClient({
    credentials: CREDENTIALS,
});

function getMsg91Dataset() {
    return bigQuery.dataset(MSG91_DATASET_ID);
}

async function getQueryResults(query: string, metadata: boolean = false) {

    const [job, info] = await getMsg91Dataset().createQueryJob({ query, location: process.env.DATA_SET_LOCATION, useQueryCache: true });
    let [rows] = await job.getQueryResults();
    if (metadata) {
        return [rows,
            {
                datasetId: info?.configuration?.query?.destinationTable?.datasetId,
                tableId: info?.configuration?.query?.destinationTable?.tableId,
                stats: {
                    processedBytes: info?.statistics?.query?.totalBytesProcessed,
                    billedBytes: info?.statistics?.query?.totalBytesBilled,
                    cacheHit: info?.statistics?.query?.cacheHit
                }
            }
        ];
    } else {
        return rows;
    }
}

async function getStream(parent: string, writeStream: { type: any }) {
    const streamName = await getWriteStreamName(parent, writeStream);
    // This header is required so that BQ storage API knows which region to route the request to
    const options = { otherArgs: { headers: { 'x-goog-request-params': `write_stream=${streamName}` } } }
    const stream = await writeClient.appendRows(options);
    return [stream, streamName]
}

async function getWriteStreamName(parent: string, writeStream: { type: any }) {
    let request: any = { parent, writeStream };
    let [response] = await writeClient.createWriteStream(request);
    console.log(`Stream created: ${response.name}`);
    return response.name;
}

export default getMsg91Dataset();
export {
    bigQuery,
    getQueryResults,
    writeClient,
    getStream
}