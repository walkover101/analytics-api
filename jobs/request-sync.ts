import logger from "../logger/logger";
import { ChangeStream, MongoClient, ObjectId, Timestamp } from 'mongodb';
import mongoService from '../database/mongo-service';
import { delay, sendChannelNotification } from "../services/utility-service";
import { DateTime } from "luxon";
import { Readable, Stream, Transform, TransformCallback } from "stream";
import ReportData from "../models/report-data.model";
import Tracker, { jobType } from "../models/trackers.model";
import RequestData from "../models/request-data.model";
import { pipeline } from "stream/promises";
const DELIVERED_STATUS_CODES = [1, 3, 26];
const REQUEST_DATA_COLLECTION = process.env.REQUEST_DATA_COLLECTION || "";
const DB_NAME = process.env.MONGO_DB_NAME;

async function handleRequestStream(stream: Stream, lastTimestamp: string, lastDocumentId: string, filterOutTimestamp: string) {

    try {
        await pipeline(stream as Readable,
            new Lag("requestDate", 48 * 60),
            new Skip(lastTimestamp, lastDocumentId),
            // new SlowDown(100),
            // new FilterOutSingleRequestWithStatus(DELIVERED_STATUS_CODES, filterOutTimestamp),
            new WriteRequest(1000)
                .on("data", async (data) => {
                    logger.info(`${data?._id} - ${data?.requestDate} - ${data?.isSingleRequest} - ${data?.reportStatus || data?.status}`);
                    logger.info(data);
                })
        )
    } catch (error: any) {
        logger.error(error);
        // await sendChannelNotification(process.env.CHANNEL_ID || "", error.message);
        await delay(2000);
        process.exit(1);
    }
}

class SlowDown extends Transform {
    time: number;
    constructor(time: number = 100, options: any = {}) {
        options.objectMode = true;
        super(options);
        this.time = time;
    }

    async _transform(request: any, encoding: BufferEncoding, callback: TransformCallback): Promise<void> {
        await delay(this.time);
        this.push(request);
        callback();
    }
}

export class Lag extends Transform {
    private field: string;
    private minute: number;
    private refreshTime: number = 5 * 1000; // in ms
    constructor(field: string, minute: number, options: any = {}) {
        options.objectMode = true;
        super(options);
        this.field = field;
        this.minute = minute;
    }

    async _transform(data: any, encoding: BufferEncoding, callback: TransformCallback): Promise<void> {
        let borderTime = DateTime.now().minus({
            minutes: this.minute
        });
        const timestamp = DateTime.fromJSDate(new Date(data?.[this.field])).toUTC();
        let diff;
        while ((diff = borderTime.diff(timestamp, 'minutes').minutes) <= 0) {
            borderTime = DateTime.now().minus({
                minutes: this.minute
            });
            logger.info("Waiting");
            await delay(this.refreshTime);
        }
        this.push(data);
        callback();
    }
}


class FilterOutSingleRequestWithStatus extends Transform {
    private status: Set<number>;
    private timestamp: DateTime;
    private startFilter: boolean = false;
    constructor(status: number[], timestamp: string, options: any = {}) {
        options.objectMode = true;
        super(options);
        this.status = new Set(status);
        // Validate timestamp
        this.timestamp = DateTime.fromJSDate(new Date(timestamp));
        if (!this.timestamp.isValid) throw new Error("Invalid filterOutDate");
    }

    _transform(data: any, encoding: BufferEncoding, callback: TransformCallback): void {

        if (this.startFilter) {
            // Filterout the records
            const status = data?.reportStatus;
            if ((data?.isSingleRequest == "1") && (this.status.has(status))) {

            } else {
                this.push(data);
            }
        } else {
            // Pass the doc to next and turn on filter if conditions are true
            let timestamp = DateTime.fromJSDate(new Date(data?.requestDate));
            let diff = timestamp.diff(this.timestamp, "milliseconds").milliseconds;
            this.push(data);
            if (diff > 0) {
                this.startFilter = true;
                logger.info("Starting Filter");
            }
        }
        callback();
    }
}

class Skip extends Transform {
    private timestamp: DateTime;
    private id: string;
    private skipped: boolean = false;
    constructor(timestamp: string, id: string, options: any = {}) {
        options.objectMode = true;
        super(options);
        this.timestamp = DateTime.fromJSDate(new Date(timestamp));
        this.id = id;
    }

    async _transform(data: any, encoding: BufferEncoding, callback: TransformCallback): Promise<void> {
        if (this.skipped) {
            this.push(data);
        } else {
            let timestamp = DateTime.fromJSDate(new Date(data?.requestDate));
            let diff = timestamp.diff(this.timestamp, "seconds").seconds;
            if (diff <= 0 && data?._id == this.id) {
                this.skipped = true;
            }
            if (diff > 0) {
                this.push(data);
                this.skipped = true;
            }

        }

        callback();

    }
}

class WriteRequest extends Transform {
    private batchSize: number;
    private batch: RequestData[] = new Array();
    private reportBatch: ReportData[] = new Array();
    constructor(batchSize: number = 100, options: any = {}) {
        options.objectMode = true;
        super(options);
        this.batchSize = batchSize;
    }

    async _transform(data: any, encoding: BufferEncoding, callback: TransformCallback): Promise<void> {
        this.batch.push(new RequestData(data));
        if (data?.isSingleRequest == "1") this.reportBatch.push(await ReportData.createAsync({ ...data, status: data?.reportStatus, sentTime: data?.requestDate, user_pid: data?.requestUserid }));
        if (this.batch.length >= this.batchSize) {
            const tasks = new Array();
            tasks.push(RequestData.insertMany(this.batch));
            if (this.reportBatch.length > 0) tasks.push(ReportData.insertMany(this.reportBatch));

            await Promise.allSettled(tasks).then((results) => {
                for (const result of results) {
                    const { status, reason }: any = result;
                    if (status == "rejected") {
                        logger.error(reason);
                        if (reason?.name == "PartialFailureError") {
                            const errors = reason?.errors || [];
                            for (const error of errors) {
                                const row = error?.row || {};
                                this.push({ ...row, error: error?.errors });
                            }
                        } else {
                            throw reason;
                        }
                    }
                }
            })

            logger.info(`Last Document : ${JSON.stringify(data?._id)}`);
            await Tracker.upsert({ jobType: jobType.REQUEST_DATA, lastTimestamp: new Date(data?.requestDate).toISOString(), lastDocumentId: data?._id?.toString() });
            this.batch = [];
            this.reportBatch = [];
        }

        callback();
    }
}

const requestSync = async (args: any) => {


    mongoService().on("connect", async (connection: MongoClient) => {
        let ts = args?.timestamp;
        let docId = args?.id;
        if (ts) {
            ts = new Date(ts).toISOString();
            try {
                if (args.force) Tracker.upsert({ jobType: jobType.REQUEST_DATA, lastTimestamp: ts, lastDocumentId: docId });
                else await Tracker.create({ jobType: jobType.REQUEST_DATA, lastTimestamp: ts, lastDocumentId: docId });

            } catch (error: any) {
                if (error.message === 'Validation error') logger.error('lastTimestamp already exists. Use --force to force replace the current value');
                else throw error;
            }
        }
        const { lastDocumentId = "", lastTimestamp }: any = await Tracker.findByPk(jobType.REQUEST_DATA);
        logger.info(`Starting data syncking from ${lastTimestamp} - ${lastDocumentId}`);
        if (!lastTimestamp) throw new Error("lastTimestamp is required");


        const collection = connection.db(DB_NAME).collection(REQUEST_DATA_COLLECTION);
        const query = {
            requestDate: {
                $gte: DateTime.fromJSDate(new Date(lastTimestamp)).toJSDate(),
                // $lte: DateTime.fromISO("2022-11-16T11:00Z").toJSDate()
            }
        }
        handleRequestStream(collection.find(query).sort({ requestDate: 1 }).stream(), lastTimestamp, lastDocumentId, "2022-11-25 09:56:25 UTC");
    });
}

// Start Syncing From - 2022-11-23 05:20:27 UTC
// Start Filtring From - 2022-11-25 09:56:25 UTC

export {
    requestSync
}