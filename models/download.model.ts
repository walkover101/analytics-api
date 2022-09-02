
import { DateTime } from 'luxon';
import { db } from '../firebase';
import { CollectionReference } from 'firebase-admin/firestore';
import logger from '../logger/logger';
import msg91Dataset from '../database/big-query-service';
import smsLogsService from '../services/sms/sms-logs-service';
import mailLogsService from '../services/email/mail-logs-service';
import otpLogsService from '../services/otp/otp-logs-service';
import { getAgeInDays } from '../services/utility-service';

export enum DOWNLOAD_STATUS {
    PENDING = 'PENDING',
    PROCESSING = 'PROCESSING',
    SUCCESS = 'SUCCESS',
    ERROR = 'ERROR'
}

export enum RESOURCE_TYPE {
    SMS = 'sms',
    EMAIL = 'mail',
    OTP = 'otp'
}

export const GCS_CSV_RETENTION = +(process.env.GCS_CSV_RETENTION || 30); // in days

const DOWNLOADS_COLLECTION = process.env.DOWNLOADS_COLLECTION || 'downloads';
const GCS_BASE_URL = process.env.GCS_BASE_URL || 'https://storage.googleapis.com';
const GCS_BUCKET_NAME = process.env.GCS_BUCKET_NAME || 'msg91-analytics';
const GCS_FOLDER_NAME = process.env.GCS_SMS_EXPORTS_FOLDER || 'sms-exports';
const DEFAULT_TIMEZONE: string = 'Asia/Kolkata';
const collection: CollectionReference = db.collection(DOWNLOADS_COLLECTION);

export default class Download {
    id?: string;
    resourceType: RESOURCE_TYPE;
    companyId: string;
    startDate: DateTime;
    endDate: DateTime;
    status: DOWNLOAD_STATUS = DOWNLOAD_STATUS.PENDING;
    timezone: string = DEFAULT_TIMEZONE;
    fields?: Array<string>;
    file?: string;
    query?: { [key: string]: string };
    err?: string;
    createdAt: Date = new Date();
    updatedAt: Date = new Date();

    constructor(resourceType: string, companyId: string, startDate: DateTime, endDate: DateTime, timezone: string, fields: string = '', query: any) {
        switch (resourceType) {
            case RESOURCE_TYPE.EMAIL:
                this.resourceType = RESOURCE_TYPE.EMAIL;
                break;
            case RESOURCE_TYPE.OTP:
                this.resourceType = RESOURCE_TYPE.OTP;
                break;
            default:
                this.resourceType = RESOURCE_TYPE.SMS;
        }

        this.companyId = companyId;
        this.startDate = startDate;
        this.endDate = endDate;
        this.query = query;
        if (timezone) this.timezone = timezone;
        if (fields && fields.length) this.fields = fields.splitAndTrim(',');
    }

    public static async index(page: number, pageSize: number, companyId?: string, resourceType?: string) {
        let query: any = collection;
        if (resourceType) query = query.where('resourceType', '==', resourceType);
        if (companyId) query = query.where('companyId', 'in', [companyId, `${companyId}`]);
        const offset = page > 1 ? (page - 1) * pageSize : 0;
        const dataSnapshot = await query.limit(pageSize).offset(offset).get();
        const countSnapshot = await query.select().get();
        const docs = dataSnapshot.docs;
        const results = docs.map((doc: any) => {
            const document = doc.data();
            document.id = doc.id;
            document.retentionStatus = this.getExpiryStatus(document.createdAt);
            document.isExpired = getAgeInDays(document.createdAt) > GCS_CSV_RETENTION;
            if (document.isExpired) document.file = null;
            return document;
        });
        return { data: results, pagination: { total: countSnapshot.docs?.length, page, pageSize } }
    }

    public save() {
        logger.info('[DOWNLOAD] Creating entry in firestore...');
        return collection.add(JSON.parse(JSON.stringify(this)));
    }

    public update(params: any) {
        if (!this.id) return;
        let { status, file, err } = params;
        const data: any = {};

        if (status) {
            data.status = status;
            logger.info(`[DOWNLOAD] Updating status to ${status}...`);
        }

        if (file) data.file = file;
        if (err) data.err = err;
        data.updatedAt = new Date().toISOString();
        return collection.doc(this.id).update(data);
    }

    public createJob(format: string = 'CSV') {
        logger.info('[DOWNLOAD] Creating job...');
        const filePath = `${GCS_BUCKET_NAME}/${GCS_FOLDER_NAME}/${this.id}`;
        const exportFilePath = `gs://${filePath}_ *.csv`;
        this.file = `${GCS_BASE_URL}/${filePath}_%20000000000000.csv`;
        let queryStatement = this.getQueryStatement();
        logger.info(`Query: ${queryStatement}`);
        return msg91Dataset.createQueryJob({ query: this.getExportQuery(this.id, queryStatement, exportFilePath, format) });
    }

    private getQueryStatement() {
        switch (this.resourceType) {
            case RESOURCE_TYPE.EMAIL:
                return mailLogsService.getQuery(this.companyId, this.startDate, this.endDate, this.timezone, this.query, this.fields);
            case RESOURCE_TYPE.OTP:
                return otpLogsService.getQuery(this.companyId, this.startDate, this.endDate, this.timezone, this.query, this.fields);
            default:
                return smsLogsService.getQuery(this.companyId, this.startDate, this.endDate, this.timezone, this.query, this.fields);
        }
    }

    private getExportQuery(downloadId: string = '', query: string, exportPath: string, format: string) {
        return `
            BEGIN
                CREATE TEMP TABLE _SESSION.${downloadId} AS (
                    WITH temptable AS (${query})
                    SELECT * FROM temptable
                );
                
                EXPORT DATA OPTIONS(
                    uri='${exportPath}',
                    format='${format}',
                    overwrite=true,
                    header=true,
                    field_delimiter=';'
                ) AS
                SELECT * FROM _SESSION.${downloadId};
            END;
        `;
    }

    private static getExpiryStatus(createdAt: string) {
        const days = GCS_CSV_RETENTION - Math.floor(getAgeInDays(createdAt));
        return days > 0 ? `Removed after ${days} days` : 'Removed'
    }
}