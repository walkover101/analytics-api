import { BigQuery } from "@google-cloud/bigquery";
import dotenv from 'dotenv';
dotenv.config();
const options = {
    keyFilename: './service-account.json',
    projectId: process.env.GCP_PROJECT_ID
}
export default new BigQuery({
    credentials: {
        "private_key": process.env.PRIVATE_KEY,
        "client_email": process.env.CLIENT_EMAIL,
        "client_id": process.env.CLIENT_ID,
    },
    "projectId": process.env.GCP_PROJECT_ID
});
