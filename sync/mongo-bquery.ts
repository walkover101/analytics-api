import { MongoClient } from 'mongodb';
import fs from 'fs';
import { DateTime } from 'luxon';
import dotenv from 'dotenv';
import bigQuery, { trimData, insertRows } from '../database/big-query';
const { dirname } = require('path');
const appDir = dirname(require.main?.filename);
const textReportSchema = ['_id', 'requestID', 'telNum', 'status', 'sentTime', 'providerSMSID', 'user_pid', 'senderID', 'smsc', 'deliveryTime', 'route', 'credit', 'retryCount', 'sentTimePeriod', 'oppri', 'crcy', 'node_id'];
dotenv.config();
const LAG = 48 * 60;  // Hours * Minutes
const INTERVAL = 5   // Minutes
// Connection URL
const url = process.env.MONGO_CONNECTION_STRING || "";
const client = new MongoClient(url);
const timestampPointerFile = `${appDir}/timestamp.txt`;
const lastDocumentProcessed = `${appDir}/last-document.txt`;
const dbName = process.env.MONGO_DB_NAME;

export async function main() {
    // console.log("Timestamp",getTimestamp());
    // Use connect method to connect to the server
    let connection = await client.connect();
    console.log('Connected successfully to server');
    const db = client.db(dbName);
    const collection = db.collection(process.env.MONGO_COLLECTION_NAME || "");
    while (true) {
        try {
            // Read the timestamp from file and set it as startTime
            const startTime = DateTime.fromISO(getLastTimestamp());
            if (!startTime.isValid) {
                throw new Error("Invalid startTime");
            }
            // Add Interval to startTime and set it as end Time
            const endTime = startTime.plus({
                minutes: INTERVAL
            });
            // Subtract the LAG from currentTime and set it as timeLimit
            const timeLimit = DateTime.now().minus({
                minutes: LAG
            });
            console.log(`Time Limit : ${timeLimit}, End Time : ${endTime}, Diff : ${timeLimit.diff(endTime, 'minute').minutes}`)
            if (timeLimit.diff(endTime, 'minute').minutes <= 0) {
                await dummyWait((INTERVAL * 1000) / 2);
            } else {
                console.log("Syncing Data...");
                const { timestamp, documentId } = await syncData(collection, startTime, endTime, getLastDocument());
                console.log(documentId);
                updatePointer(timestamp.toString(), documentId || undefined);
                await dummyWait(100);
            }
        } catch (error) {
            console.error(error);
            await dummyWait(10000);
        }

    }
}

// main()
//     .then(console.log)
//     .catch(console.error)
//     .finally(() => client.close());

async function syncData(collection: any, startTime: DateTime, endTime: DateTime, docuemntId?: string) {
    const output = {
        documentId: null,
        timestamp: endTime
    }
    const query = {
        sentTime: {
            $gte: startTime,
            $lte: endTime
        }
    }
    const docs = await collection.find(query).sort({ sentTime: 1 }).toArray();
    // console.log(apps);
    let skip = !!docuemntId;
    for (let i = 0; i < docs.length; i++) {
        const doc = docs[i];
        // Skip documents that have already been processed
        if (skip) {
            if (doc._id == docuemntId) {
                skip = false;
            } else {
                skip = true;
            }
            continue;
        }

        await insertRows("msg91_test", "report_data", [trimData(textReportSchema, { ...doc, _id: doc?._id?.toString() })]);
        // Update the pointer to the last processed document
        let timestamp = DateTime.fromJSDate(doc.sentTime);
        if (timestamp?.isValid) {
            output.timestamp = timestamp;
        }
        output.documentId = doc["_id"]?.toString();
    }
    return output;
}

function updatePointer(timestamp: string, documentId: string = 'null') {
    try {
        fs.writeFileSync(timestampPointerFile, timestamp);
        fs.writeFileSync(lastDocumentProcessed, documentId || 'null');
    } catch (error) {
        throw error;
    }
}
function getLastTimestamp() {
    try {
        let data = fs.readFileSync(timestampPointerFile, 'utf-8');
        return data.trim();
    } catch (error) {
        console.error(error);
        throw new Error("Please set the initial timestamp to sync data from in timestamp.txt file");
    }
}
/**
 * 
 * @returns Returns the last processed document
 */
function getLastDocument() {
    try {
        let data = fs.readFileSync(lastDocumentProcessed, 'utf-8');
        if (data && data != 'null') {
            return data.trim();
        } else {
            return undefined;
        }
    } catch (error) {
        return undefined;
    }
}
function dummyWait(timeInMS: number) {
    return new Promise((resolve, reject) => {
        setTimeout(() => {
            return resolve(true);
        }, timeInMS);
    });
}
function getCurrentTimeInUTC() {
    return DateTime.now().toUTC().toUnixInteger();
}

