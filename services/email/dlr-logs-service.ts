import { Table } from '@google-cloud/bigquery';
import msg91Dataset from '../../database/big-query-service';
import logger from '../../logger/logger';
import DlrLog from '../../models/dlr-log.model';
import Download from '../../models/download.model';
import { getQuotedStrings, getValidFields } from '../utility-service';

const DLR_LOGS_TABLE_ID = process.env.DLR_LOGS_TABLE_ID || 'dlr_logs'
const GCS_BUCKET_NAME = 'msg91-analytics';
const GCS_FOLDER_NAME = 'email-exports';
const PERMITTED_FIELDS: { [key: string]: string } = {
    senderDedicatedIpId: 'dlrLog.senderDedicatedIpId',
    statusCode: 'dlrLog.statusCode',
    enhancedStatusCode: 'dlrLog.enhancedStatusCode',
    resultState: 'dlrLog.resultState',
    reason: 'dlrLog.reason',
    remoteMx: 'dlrLog.remoteMx',
    remoteIp: 'dlrLog.remoteIp',
    contentSize: 'dlrLog.contentSize',
    diffDeliveryTime: 'dlrLog.diffDeliveryTime',
    hostname: 'dlrLog.hostname',
    outboundEmailId: 'dlrLog.outboundEmailId',
    recipientEmail: 'dlrLog.recipientEmail',
    uid: 'dlrLog.uid',
    companyId: 'dlrLog.companyId',
    subject: 'dlrLog.subject',
    domain: 'dlrLog.domain',
    senderEmail: 'dlrLog.senderEmail',
    mailTypeId: 'dlrLog.mailTypeId',
    templateSlug: 'dlrLog.templateSlug',
    mailerRequestId: 'dlrLog.mailerRequestId',
    campaignId: 'dlrLog.campaignId',
    clientRequestIp: 'dlrLog.clientRequestIp',
    createdAt: 'dlrLog.createdAt',
    updatedAt: 'dlrLog.updatedAt'
};

class DlrLogsService {
    private static instance: DlrLogsService;
    private dlrLogsTable: Table;

    constructor() {
        this.dlrLogsTable = msg91Dataset.table(DLR_LOGS_TABLE_ID);
    }

    public static getSingletonInstance(): DlrLogsService {
        return DlrLogsService.instance ||= new DlrLogsService();
    }

    public insertMany(rows: Array<DlrLog>) {
        const insertOptions = { skipInvalidRows: true, ignoreUnknownValues: true };
        return this.dlrLogsTable.insert(rows, insertOptions);
    }

    public download(download: Download, format: string = 'CSV') {
        logger.info('[DOWNLOAD] Creating job...');
        const exportFilePath = `gs://${GCS_BUCKET_NAME}/${GCS_FOLDER_NAME}/${download.id}_*.csv.gz`;
        const fields = getValidFields(PERMITTED_FIELDS, download.fields).join(',');
        const whereClause = this.getWhereClause(download);
        const queryStatement = `select ${fields} from ${DLR_LOGS_TABLE_ID} as dlrLog WHERE ${whereClause}`;
        logger.info(`Query: ${queryStatement}`);
        return msg91Dataset.createQueryJob({ query: this.prepareExportQuery(download.id, queryStatement, exportFilePath, format) });
    }

    private getWhereClause(download: Download) {
        const query: { [key: string]: string } = download.query || {};

        // mandatory conditions
        let conditions = `dlrLog.companyId = ${download.companyId}`;
        conditions += ` AND (DATE(dlrLog.createdAt) BETWEEN "${download.startDate.toFormat('yyyy-MM-dd')}" AND "${download.endDate.toFormat('yyyy-MM-dd')}")`;

        // optional conditions
        if (query.senderDedicatedIpId) conditions += ` AND dlrLog.senderDedicatedIpId in (${query.senderDedicatedIpId.split(',')})`;
        if (query.eventId) conditions += ` AND dlrLog.eventId in (${query.eventId.split(',')})`;
        if (query.remoteMx) conditions += ` AND dlrLog.remoteMx in (${getQuotedStrings(query.remoteMx.split(','))})`;
        if (query.remoteIp) conditions += ` AND dlrLog.remoteIp in (${getQuotedStrings(query.remoteIp.split(','))})`;
        if (query.hostname) conditions += ` AND dlrLog.hostname in (${getQuotedStrings(query.hostname.split(','))})`;
        if (query.recipientEmail) conditions += ` AND dlrLog.recipientEmail in (${getQuotedStrings(query.recipientEmail.split(','))})`;
        if (query.domain) conditions += ` AND dlrLog.domain in (${getQuotedStrings(query.domain.split(','))})`;
        if (query.senderEmail) conditions += ` AND dlrLog.senderEmail in (${getQuotedStrings(query.senderEmail.split(','))})`;
        if (query.mailTypeId) conditions += ` AND dlrLog.mailTypeId in (${query.mailTypeId.split(',')})`;
        if (query.templateSlug) conditions += ` AND dlrLog.templateSlug in (${getQuotedStrings(query.templateSlug.split(','))})`;
        if (query.mailerRequestId) conditions += ` AND dlrLog.mailerRequestId in (${getQuotedStrings(query.mailerRequestId.split(','))})`;
        if (query.campaignId) conditions += ` AND dlrLog.campaignId in (${query.campaignId.split(',')})`;
        if (query.subject) conditions += ` AND UPPER(dlrLog.subject) LIKE '%${query.subject.toUpperCase()}%'`;

        return conditions;
    }

    private prepareExportQuery(downloadId: string = '', query: string, exportPath: string, format: string) {
        return `
            BEGIN
                CREATE TEMP TABLE _SESSION.${downloadId} AS (
                    WITH temptable AS (${query})
                    SELECT * FROM temptable
                );
                
                EXPORT DATA OPTIONS(
                    uri='${exportPath}',
                    format='${format}',
                    compression='GZIP',
                    overwrite=true,
                    header=true,
                    field_delimiter=';'
                ) AS
                SELECT * FROM _SESSION.${downloadId};
            END;
        `;
    }
}

export default DlrLogsService.getSingletonInstance();
