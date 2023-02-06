import logger from "../logger/logger";
import { ChangeStream, MongoClient, ObjectId, Timestamp } from 'mongodb';
import mongoService from '../database/mongo-service';
import { delay } from "../services/utility-service";
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
const DELIVERED_STATUS_CODES = [1, 3, 26];

async function handleStream(stream: ChangeStream) {
    logger.info("Handling Stream");
    const readableStream: Readable = stream.stream();

    try {

        await pipeline(readableStream,
            new FilterOperation(["insert", "update"]),
            new FilterStatus(DELIVERED_STATUS_CODES),
            new FilterSingleRequest(),
            new FilterUpdates(["reportStatus"]),
            new WriteRequest(100)
                .on("data", async (request) => {

                    logger.info(JSON.stringify(request));
                    // "updateDescription":{"updatedFields":{"isCopied":1}
                    // logger.info(JSON.stringify(request?.updateDescription?.updatedFields));
                    // logger.info(JSON.stringify(request));
                }
                )
        );
    } catch (error: any) {
        logger.error(error);
        // await sendChannelNotification(process.env.CHANNEL_ID || "", error.message);
        process.exit(1);
    }

}


const rtRequestSync = async (args: any) => {
    mongoService().on("connect", async (connection: MongoClient) => {
        let lastToken = args?.token;
        if (lastToken) {
            try {
                if (args.force) await Tracker.upsert({ jobType: jobType.RT_REQUEST_DATA, token: lastToken, lastTimestamp: new Date().toISOString() });
                else await Tracker.create({ jobType: jobType.RT_REQUEST_DATA, token: lastToken, lastTimestamp: new Date().toISOString() });
            } catch (error: any) {
                if (error.message === 'Validation error') logger.error('token already exists. Use --force to force replace the current value');
                else throw error;
            }
        }

        const { token = "", lastTimestamp }: any = await Tracker.findByPk(jobType.RT_REQUEST_DATA);
        if (!token) {
            logger.info(`token not found. Will start syncing from now after 10 seconds.`)
            await delay(10000);
        }
        logger.info(`Starting data syncing from ${lastTimestamp} - ${token}`);

        const collection = connection.db(DB_NAME).collection(REQUEST_DATA_COLLECTION);
        const options: any = {
            fullDocument: 'updateLookup',
            batchSize: 2
        }
        if (token) options.startAfter = { "_data": token }
        const changeStream = collection.watch([], options);
        await handleStream(changeStream);
    });
}

export {
    rtRequestSync
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

class FilterStatus extends Transform {
    private status: Set<number>;
    constructor(status: number[], options: any = {}) {
        options.objectMode = true;
        super(options);
        this.status = new Set(status);
    }
    _transform(request: any, encoding: BufferEncoding, callback: TransformCallback): void {
        const status = request?.fullDocument?.reportStatus;
        if (this.status.has(status)) {
            // logger.info(status);
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
