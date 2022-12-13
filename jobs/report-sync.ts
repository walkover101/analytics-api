import logger from "../logger/logger";
import { ChangeStream, MongoClient, ObjectId, Timestamp } from 'mongodb';
import mongoService from '../database/mongo-service';
import { delay, sendChannelNotification } from "../services/utility-service";
import { DateTime } from "luxon";
import { Readable, Stream, Transform, TransformCallback } from "stream";
import ReportData from "../models/report-data.model";
import Tracker, { jobType } from "../models/trackers.model";
import RequestData from "../models/request-data.model";
import { Lag } from "./request-sync";
import { pipeline } from "stream/promises";
import rabbitmqProducer from "../database/rabbitmq-producer";
const REPORT_DATA_COLLECTION = process.env.REPORT_DATA_COLLECTION || "";
const DB_NAME = process.env.MONGO_DB_NAME;

async function handleReportStream(stream: Stream, lastTimestamp: string, lastDocumentId: string, persist: boolean = true) {
    try {
        await pipeline(
            stream as Readable,
            new Lag("sentTime", 48 * 60),
            new Skip(lastTimestamp, lastDocumentId),
            new WriteReport(1000,persist)
                .on("data", (data) => {
                    logger.info(JSON.stringify(data))
                }));

    } catch (error: any) {
        logger.error(error);
        await sendChannelNotification(process.env.CHANNEL_ID || "", error.message).catch(error => {
            console.log(error);
        });;
        await delay(2000);
        process.exit(1);

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
            // await Tracker.upsert({ jobType: jobType.REPORT_DATA, lastTimestamp: new Date(data?.sentTime).toISOString(), lastDocumentId: data?._id?.toString() })
            // await delay(1000);
        } else {
            let timestamp = DateTime.fromJSDate(new Date(data?.sentTime));
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

class WriteReport extends Transform {
    private batchSize: number;
    private batch: ReportData[] = new Array();
    private persist: boolean;
    constructor(batchSize: number = 100,persist: boolean = true, options: any = {}) {
        options.objectMode = true;
        super(options);
        this.batchSize = batchSize;
        this.persist = persist;
    }

    async _transform(data: any, encoding: BufferEncoding, callback: TransformCallback): Promise<void> {
        if (this.writableEnded && this.writableLength == 1) this.batchSize = 1;
        
        this.batch.push(await ReportData.createAsync(data));
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
            logger.info(`${data?._id} - ${data?.sentTime} `);

            if(this.persist) await Tracker.upsert({ jobType: jobType.REPORT_DATA, lastTimestamp: new Date(data?.sentTime).toISOString(), lastDocumentId: data?._id?.toString() });
            this.batch = [];
        }
        callback();
    }
}

const reportSync = async (args: any) => {
    mongoService().on("connect", async (connection: MongoClient) => {
        let ts = args?.timestamp;
        let docId = args?.id;
        if (ts) {
            try {
                if (args.force) await Tracker.upsert({ jobType: jobType.REPORT_DATA, lastTimestamp: ts, lastDocumentId: docId });
                else await Tracker.create({ jobType: jobType.REPORT_DATA, lastTimestamp: ts, lastDocumentId: docId });
            } catch (error: any) {
                if (error.message === 'Validation error') logger.error('lastTimestamp already exists. Use --force to force replace the current value');
                else throw error;
            }
        }
        const { lastDocumentId = "", lastTimestamp }: any = await Tracker.findByPk(jobType.REPORT_DATA);
        logger.info(`Starting data syncing from ${lastTimestamp} - ${lastDocumentId}`);
        if (!lastTimestamp) throw new Error("lastTimestamp is required");


        const collection = connection.db(DB_NAME).collection(REPORT_DATA_COLLECTION);
        const query = {
            sentTime: {
                $gte: DateTime.fromJSDate(new Date(lastTimestamp)).toJSDate(),
                // $lte: DateTime.fromISO("2022-11-16T11:00Z").toJSDate()
            }
        }
        await handleReportStream(collection.find(query).sort({ sentTime: 1 }).stream(), lastTimestamp, lastDocumentId)
    });
}

const reportPatch = async (args: any) => {


    mongoService().on("connect", async (connection: MongoClient) => {
        let start = new Date(args?.start_ts).toISOString();
        let end = new Date(args?.end_ts).toISOString();
        let docId = args?.id;
        logger.info(`Patching data between ${start} AND ${end}`);
        if (!start) throw new Error("start_ts is required");
        if (!end) throw new Error("end_ts is required");

        const collection = connection.db(DB_NAME).collection(REPORT_DATA_COLLECTION);
        const query = {
            sentTime: {
                $gte: DateTime.fromJSDate(new Date(start)).toJSDate(),
                $lte: DateTime.fromJSDate(new Date(end)).toJSDate()
            }
        }
        await handleReportStream(collection.find(query).sort({ sentTime: 1 }).stream(), start, docId,false)
  
        // const collection = connection.db(DB_NAME).collection(REQUEST_DATA_COLLECTION);
        // const query = {
        //     requestDate: {
        //         $gte: DateTime.fromJSDate(new Date(start)).toJSDate(),
        //         $lte: DateTime.fromJSDate(new Date(end)).toJSDate()
        //     }
        // }
        // handleRequestStream(collection.find(query).sort({ requestDate: 1 }).stream(), start, docId, false);
    });
}


export {
    reportSync,
    reportPatch
}