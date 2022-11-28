import logger from "../logger/logger";
import { ChangeStream, MongoClient, ObjectId, Timestamp } from 'mongodb';
import mongoService from '../database/mongo-service';
import { delay, sendChannelNotification } from "../services/utility-service";
import { DateTime } from 'luxon';
import Tracker, { jobType } from "../models/trackers.model";
import ReportData from '../models/report-data.model';
import RequestData from '../models/request-data.model';
import { Readable, TransformCallback } from "stream";
import { Transform } from "stream";
import { pipeline } from "stream/promises";

const REQUEST_DATA_COLLECTION = process.env.REQUEST_DATA_COLLECTION || '';
const REPORT_DATA_COLLECTION = process.env.REPORT_DATA_COLLECTION || '';
const DB_NAME = process.env.MONGO_DB_NAME;

async function handleRequestStream(changeStream: ChangeStream) {
    const readableStream: Readable = changeStream.stream();
    try {
        await pipeline(readableStream,
            new FilterOperation(["insert", "update"]),
            new FilterUpdates([]),
            new SlowDown(500),
            new Log()
        )
    } catch (error: any) {
        logger.error(error);
        await sendChannelNotification(process.env.CHANNEL_ID || "", error.message);
        process.exit(1);
    }

}

async function handleReportStream(changeStream: ChangeStream) {
    const readableStream: Readable = changeStream.stream();
    try {
        await pipeline(readableStream,
            new FilterOperation(["insert", "update"]),
            new FilterUpdates([]),
            new SlowDown(500),
            new Log()
        )
    } catch (error: any) {
        logger.error(error);
        await sendChannelNotification(process.env.CHANNEL_ID || "", error.message);
        process.exit(1);
    }
}
async function initToken(args: any, job: jobType) {
    let startToken = args?.token;
    if (startToken) {
        try {
            if (args.force) await Tracker.upsert({ jobType: job, token: startToken, lastTimestamp: new Date().toISOString() });
            else await Tracker.create({ jobType: job, token: startToken, lastTimestamp: new Date().toISOString() });
        } catch (error: any) {
            if (error.message === 'Validation error') logger.error('token already exists. Use --force to force replace the current value');
            else throw error;
        }
    }
}
export const rtRequestSync = async (args: any) => {
    if (!DB_NAME) throw new Error("DB_NAME is not found in env")
    if (!REQUEST_DATA_COLLECTION) throw new Error("REQUEST_DATA_COLLECTION is not found in env");
    await initToken(args, jobType.RT_REQUEST_DATA);
    mongoService().on("connect", async (connection: MongoClient) => {
        const { token = "", lastTimestamp }: any = await Tracker.findByPk(jobType.RT_REQUEST_DATA);
        const collection = connection.db(DB_NAME).collection(REQUEST_DATA_COLLECTION);
        const options: any = {
            fullDocument: 'updateLookup',
            batchSize: 2,
            startAfter: (token) ? { "_data": token } : null
        }
        const stream = collection.watch([], options);
        await handleRequestStream(stream);
    })
}
export const rtReportSync = async (args: any) => {
    if (!DB_NAME) throw new Error("DB_NAME is not found in env")
    if (!REPORT_DATA_COLLECTION) throw new Error("REPORT_DATA_COLLECTION is not found in env");
    await initToken(args, jobType.RT_REPORT_DATA);
    mongoService().on("connect", async (connection: MongoClient) => {
        const { token = "", lastTimestamp }: any = await Tracker.findByPk(jobType.RT_REQUEST_DATA);
        const collection = connection.db(DB_NAME).collection(REPORT_DATA_COLLECTION);
        const options: any = {
            fullDocument: 'updateLookup',
            batchSize: 2,
            startAfter: (token) ? { "_data": token } : null
        }
        const stream = collection.watch([], options);
        await handleReportStream(stream);
    })
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
class Log extends Transform {
    private key;
    constructor(key?: string, options: any = {}) {
        options.objectMode = true;
        super(options);
        this.key = key;
    }

    async _transform(request: any, encoding: BufferEncoding, callback: TransformCallback): Promise<void> {
        if (this.key) {
            logger.info(request[this.key]);
        } else {
            logger.info(request);
        }
        this.push(request);
        callback();
    }
}

class FilterSingleRequest extends Transform {
    constructor(options: any = {}) {
        options.objectMode = true;
        super(options);
    }

    _transform(request: any, encoding: BufferEncoding, callback: TransformCallback): void {
        const fullDocument = request?.fullDocument;
        if (fullDocument?.isSingleRequest == "1") {
            // logger.info(`Status : ${fullDocument?.isSingleRequest}`)
            this.push(request);
        }
        callback();
    }
}

// Only pass further if 
class FilterOperation extends Transform {
    operations: Set<string>;
    constructor(operations: string[], options: any = {}) {
        options.objectMode = true;
        super(options);
        this.operations = new Set(operations);
    }
    _transform(request: any, encoding: BufferEncoding, callback: TransformCallback): void {
        const op = request?.operationType;
        // logger.info(op);
        if (this.operations.has(op)) {
            this.push(request);
        }
        callback();
    }
}

class FilterUpdates extends Transform {
    fields: string[];
    constructor(fields: string[], options: any = {}) {
        options.objectMode = true;
        super(options);
        this.fields = fields;
    }

    _transform(request: any, encoding: BufferEncoding, callback: TransformCallback): void {
        // "updateDescription":{"updatedFields":{"isCopied":1}
        const op = request?.operationType;
        if (op != "update") {
            this.push(request);
        } else {
            // const updatedFields = request?.updateDescription?.updatedFields;
            const updatedFields = new Set(Object.keys(request?.updateDescription?.updatedFields || []));
            const isAllowedField = this.fields.some((field) => updatedFields.has(field));
            if (isAllowedField) {
                this.push(request);
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
        let doc = data?.fullDocument;
        this.batch.push(new RequestData(doc));
        if (doc?.isSingleRequest == "1") this.reportBatch.push(await ReportData.createAsync({ ...doc, status: doc?.reportStatus, sentTime: doc?.requestDate, user_pid: doc?.requestUserid }));
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

            logger.info(`Last Pointer : ${JSON.stringify(data?._id)}`);
            await Tracker.upsert({ jobType: jobType.RT_REQUEST_DATA, lastTimestamp: new Date(doc?.requestDate).toISOString(), token: data?._id?._data });
            this.batch = [];
            this.reportBatch = [];
        }

        callback();
    }
}

class WriteReport extends Transform {
    private batchSize: number;
    private batch: ReportData[] = new Array();
    constructor(batchSize: number = 100, options: any = {}) {
        options.objectMode = true;
        super(options);
        this.batchSize = batchSize;
    }

    async _transform(data: any, encoding: BufferEncoding, callback: TransformCallback): Promise<void> {
        const doc = data?.fullDocument;
        this.batch.push(await ReportData.createAsync(doc));
        if (this.batch.length >= this.batchSize) {
            await ReportData.insertMany(this.batch).catch((reason) => {
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
            });
            await Tracker.upsert({ jobType: jobType.RT_REPORT_DATA, lastTimestamp: new Date(doc?.sentTime).toISOString(), token: data?._id?._data });
            this.batch = [];
        }
        callback();
    }
}