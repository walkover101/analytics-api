
import { DateTime } from 'luxon';
import { db } from '../firebase';
import logger from '../logger/logger';
import msg91Dataset from '../database/big-query-service';
import smsLogsService from '../services/sms/sms-logs-service';
import mailLogsService from '../services/email/mail-logs-service';
import otpLogsService from '../services/otp/otp-logs-service';
import { getAgeInDays } from '../services/utility-service';
import waLogsService from '../services/whatsapp/wa-logs-service';
import smsAnalyticsService from '../services/sms/sms-analytics-service';
import mailAnalyticsService from '../services/email/mail-analytics-service';

export enum DOWNLOAD_STATUS {
    PENDING = 'PENDING',
    PROCESSING = 'PROCESSING',
    SUCCESS = 'SUCCESS',
    ERROR = 'ERROR'
}

export enum RESOURCE_TYPE {
    SMS = 'sms',
    EMAIL = 'mail',
    OTP = 'otp',
    WA = 'wa'
}

export enum REPORT_TYPE {
    ANALYTICS = 'analytics',
    LOGS = 'logs'
}

export const GCS_CSV_RETENTION = +(process.env.GCS_CSV_RETENTION || 30); // in days

const DOWNLOADS_COLLECTION = process.env.DOWNLOADS_COLLECTION || 'downloads';
const GCS_BUCKET_NAME = process.env.GCS_BUCKET_NAME || 'msg91-analytics';
const DEFAULT_TIMEZONE: string = 'Asia/Kolkata';
const getCollectionName = (reportType: REPORT_TYPE = REPORT_TYPE.LOGS) => `${DOWNLOADS_COLLECTION}_${reportType}`;
const getCollection = (reportType: REPORT_TYPE = REPORT_TYPE.LOGS) => db.collection(getCollectionName(reportType));

export default class Download {
    id?: string;
    resourceType: RESOURCE_TYPE;
    reportType: REPORT_TYPE;
    companyId: string;
    startDate: DateTime;
    endDate: DateTime;
    status: DOWNLOAD_STATUS = DOWNLOAD_STATUS.PENDING;
    timezone: string = DEFAULT_TIMEZONE;
    fields?: Array<string>;
    file?: string;
    zipInfo?: { bucket: string, srcFolder: string, destFileName: string, firebase: { collection: string, id: string } };
    query?: { [key: string]: string };
    err?: string;
    createdAt: Date = new Date();
    updatedAt: Date = new Date();

    constructor(reportType: REPORT_TYPE, resourceType: string, companyId: string, startDate: DateTime, endDate: DateTime, timezone: string, fields: string = '', query: any) {
        switch (resourceType) {
            case RESOURCE_TYPE.EMAIL:
                this.resourceType = RESOURCE_TYPE.EMAIL;
                break;
            case RESOURCE_TYPE.OTP:
                this.resourceType = RESOURCE_TYPE.OTP;
                break;
            case RESOURCE_TYPE.WA:
                this.resourceType = RESOURCE_TYPE.WA;
                break;
            default:
                this.resourceType = RESOURCE_TYPE.SMS;
        }

        this.reportType = reportType;
        this.companyId = companyId;
        this.startDate = startDate;
        this.endDate = endDate;
        this.query = query;
        if (timezone) this.timezone = timezone;
        if (fields && fields.length) this.fields = fields.splitAndTrim(',');
    }

    public static async index(reportType: REPORT_TYPE, page: number, pageSize: number, companyId?: string, resourceType?: string) {
        let query: any = getCollection(reportType);
        if (resourceType) query = query.where('resourceType', '==', resourceType);
        if (companyId) query = query.where('companyId', 'in', [companyId, `${companyId}`]);
        const offset = page > 1 ? (page - 1) * pageSize : 0;
        const dataSnapshot = await query.orderBy('createdAt', 'desc').limit(pageSize).offset(offset).get();
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
        return getCollection(this.reportType).add(JSON.parse(JSON.stringify(this)));
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
        return getCollection(this.reportType).doc(this.id).update(data);
    }

    public createJob(format: string = 'CSV') {
        logger.info('[DOWNLOAD] Creating job...');
        const srcFolder = `${this.reportType || REPORT_TYPE.LOGS}/${this.resourceType || 'default'}/${this.id}`;
        const destFileName = `${this.companyId}_${this.startDate?.toFormat('dd-MM-yyyy')}_${this.endDate?.toFormat('dd-MM-yyyy')}_${this.id}`;
        const exportFilePath = `gs://${GCS_BUCKET_NAME}/${srcFolder}/${destFileName}_*.csv`;
        this.zipInfo = { bucket: GCS_BUCKET_NAME, srcFolder, destFileName, firebase: { collection: getCollectionName(this.reportType), id: this.id || '' } }
        let queryStatement = this.reportType === REPORT_TYPE.ANALYTICS ? this.getAnalyticsQueryStatement() : this.getLogsQueryStatement();
        return msg91Dataset.createQueryJob({ query: this.getExportQuery(this.id, queryStatement, exportFilePath, format) });
    }

    private getLogsQueryStatement() {
        switch (this.resourceType) {
            case RESOURCE_TYPE.EMAIL:
                return mailLogsService.getQuery(this.companyId, this.startDate, this.endDate, this.timezone, this.query, this.fields);
            case RESOURCE_TYPE.OTP:
                return otpLogsService.getQuery(this.companyId, this.startDate, this.endDate, this.timezone, this.query, this.fields);
            case RESOURCE_TYPE.WA:
                return waLogsService.getQuery(this.companyId, this.startDate, this.endDate, this.timezone, this.query, this.fields);
            default:
                return smsLogsService.getQuery(this.companyId, this.startDate, this.endDate, this.timezone, this.query, this.fields);
        }
    }

    private getAnalyticsQueryStatement() {
        switch (this.resourceType) {
            case RESOURCE_TYPE.EMAIL:
                return mailAnalyticsService.getQuery(this.companyId, this.startDate, this.endDate, this.timezone, this.query, this.query?.groupBy);
            default:
                return smsAnalyticsService.getQuery(this.companyId, this.startDate, this.endDate, this.timezone, this.query, this.query?.groupBy);
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
                    field_delimiter=','
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