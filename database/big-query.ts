import { BigQuery } from "@google-cloud/bigquery";
import dotenv from 'dotenv';
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
export async function insertRow(datasetId: string, tableId: string, rows: Array<Object>) {
    try {
        await bigQuery.dataset(datasetId).table(tableId).insert(rows);
        console.log(`Inserted ${rows.length} rows`);
    } catch (error) {
        console.log(rows);
        console.error(JSON.stringify(error));
    }
}

export function trimData(schema: Array<string>, data: { [key: string]: any }) {
    let output: { [key: string]: any } = {};
    schema.forEach((key: string) => {
        output[key as string] = data[key as string];
    })
    return output;
}
