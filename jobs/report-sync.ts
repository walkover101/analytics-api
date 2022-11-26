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

async function handleReportStream(stream: Stream, lastTimestamp: string, lastDocumentId: string) {
    try {
        await pipeline(
            stream as Readable,
            new Lag("sentTime", 55 * 60),
            new Skip(lastTimestamp, lastDocumentId),
            new WriteReport(1000)
                .on("data", (data) => {
                    logger.info(JSON.stringify(data))
                }));

    } catch (error: any) {
        logger.error(error);
        await sendChannelNotification(process.env.CHANNEL_ID || "", error.message);
        await delay(2000);
        process.exit(1);

    }
    // stream
    //     .pipe(new Lag("sentTime", 55 * 60))
    //     .pipe(new Skip(lastTimestamp, lastDocumentId))
    //     // .pipe(new WriteReport(1000))
    //     // .on("data", async (data) => {
    //     //     logger.info(data?.sentTime);


    //     // })
    //     .on("error", (error) => {
    //         logger.error(error);
    //     })
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
        // logger.info(data?._id, data?.sentTime);
        // await delay(500);
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
    constructor(batchSize: number = 100, options: any = {}) {
        options.objectMode = true;
        super(options);
        this.batchSize = batchSize;
    }

    async _transform(data: any, encoding: BufferEncoding, callback: TransformCallback): Promise<void> {
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
            await Tracker.upsert({ jobType: jobType.REPORT_DATA, lastTimestamp: new Date(data?.sentTime).toISOString(), lastDocumentId: data?._id?.toString() });
            this.batch = [];
        }
        callback();
    }
}

const reportSync = async (args: any) => {
    mongoService().on("connect", async (connection: MongoClient) => {
        let ts = args?.timestamp;
        if (ts) {
            try {
                if (args.force) await Tracker.upsert({ jobType: jobType.REPORT_DATA, lastTimestamp: ts });
                else await Tracker.create({ jobType: jobType.REPORT_DATA, lastTimestamp: ts });
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

export {
    reportSync
}