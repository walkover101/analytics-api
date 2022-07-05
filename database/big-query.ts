import { BigQuery } from "@google-cloud/bigquery";
import dotenv from 'dotenv';
import fs from 'fs';
dotenv.config();
const options = {
    keyFilename: './service-account.json',
    projectId: process.env.GCP_PROJECT_ID
}
const bigQuery = new BigQuery({
    credentials: {
        "private_key": process.env.PRIVATE_KEY,
        "client_email": process.env.CLIENT_EMAIL,
        "client_id": process.env.CLIENT_ID,
    },
    "projectId": process.env.GCP_PROJECT_ID
});
export default bigQuery;
export async function insertRows(datasetId: string, tableId: string, rows: Array<Object>) {
    await bigQuery.dataset(datasetId).table(tableId).insert(rows, { skipInvalidRows: true, ignoreUnknownValues: true }).then(value => {
        console.log(`Inserted ${rows.length} rows`);

    }).catch((err) => {
        if (err.name === 'PartialFailureError') {
            // Some rows failed to insert, while others may have succeeded.
            console.log("Some rows have failed to insert");
            console.log(err?.message);
            // err.errors (object[]):
            // err.errors[].row (original row object passed to `insert`)
            // err.errors[].errors[].reason
        } 
        console.error(JSON.stringify(err));
    });
}

export function trimData(schema: Array<string>, data: { [key: string]: any }) {
    let output: { [key: string]: any } = {};
    schema.forEach((key: string) => {
        output[key as string] = data[key as string];
    })
    return output;
}
