import { MongoClient } from 'mongodb';
import fs from 'fs';
import { DateTime } from 'luxon';
import dotenv from 'dotenv';
dotenv.config();
const LAG = 48 * 60;  // Hours * Minutes
const INTERVAL = 10   // Minutes 
// Connection URL
const url = process.env.MONGO_CONNECTION_STRING || "";
const client = new MongoClient(url);
const timestampPointerFile = "../timestamp.txt";
const lastDocumentProcessed = "../last-document.txt";
const dbName = process.env.MONGO_DB_NAME;

async function main() {
    // console.log("Timestamp",getTimestamp());
    // Use connect method to connect to the server
    let connection = await client.connect();
    console.log('Connected successfully to server');
    const db = client.db(dbName);
    const collection = db.collection(process.env.MONGO_COLLECTION_NAME || "");
    const prevDate = DateTime.fromISO("2022-01-29T11:11:08.224+00:00").toUTC().toUnixInteger();
    while (true) {
        try {
            // Read the timestamp from file and set it as startTime
            var startTime = DateTime.fromISO(getLastTimestamp());
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
                await dummyWait(5000);
            } else {
                console.log("Syncing Data...");
                const { timestamp, documentId } = await syncData(collection, startTime, endTime, getLastDocument());
                console.log(documentId);
                updatePointer(timestamp.toString(), documentId || undefined);
                await dummyWait(1000);
            }
        } catch (error) {
            console.log(error);
            await dummyWait(10000);
        }

    }
}

main()
    .then(console.log)
    .catch(console.error)
    .finally(() => client.close());

async function syncData(collection: any, startTime: DateTime, endTime: DateTime, docuemntId?: string) {
    const output = {
        documentId: null,
        timestamp: endTime
    }
    const query = {
        sentTime: {
            $gte: '',
            $lt: ''
        }
    }
    const apps = await collection.find({
        updatedAt: {
            $gte: startTime,
            $lte: endTime
        }
    }).sort({ updatedAt: 1 }).limit(2).toArray();
    // console.log(apps);
    let skip = !!docuemntId;
    for (let i = 0; i < apps.length; i++) {
        const app = apps[i];
        // Skip documents that have already been processed
        if (skip) {
            console.log("Skiping", app);
            if (app._id == docuemntId) {
                skip = false;
            } else {
                skip = true;
            }
            continue;
        }
        // TODO: ANKIT
        // Add data to big query
        // Handle main errors
        // Break the loop if something is wrong with network
        // Continue if error occurred because of data was wrong and push that data to a file

        // Update the pointer to the last processed document
        output.timestamp = DateTime.fromJSDate(app.updatedAt);
        if (!output.timestamp?.isValid) {
            output.timestamp = endTime;
        }
        output.documentId = app["_id"]?.toString();
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
        return data;
    } catch (error) {
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
            return data;
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