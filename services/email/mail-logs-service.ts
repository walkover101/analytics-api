import { getQueryResults, MAIL_REP_TABLE_ID, MAIL_REQ_TABLE_ID, MSG91_PROJECT_ID } from '../../database/big-query-service';
import { convertCodesToMessage, getQuotedStrings, getValidFields } from '../utility-service';
import { DateTime } from 'luxon';
import logger from '../../logger/logger';

const STATUS_CODES = {
    1: "Queued",
    2: "Accepted",
    3: "Rejected",
    4: "Delivered",
    5: "Opened",
    6: "Unsubscribed",
    7: "Clicked",
    8: "Bounced",
    9: "Failed",
    10: "Complaints",
}
const MAIL_TYPES = {
    1: "Transactional",
    2: "Notification",
    3: "Promotional",
}
const DEFAULT_TIMEZONE: string = 'Asia/Kolkata';
const PERMITTED_FIELDS: { [key: string]: string } = {
    // Mail Request
    createdAt: `STRING(TIMESTAMP_TRUNC(DATETIME(mailRequest.createdAt,'${DEFAULT_TIMEZONE}'), SECOND))`,
    requestId: 'mailRequest.requestId',
    subject: 'mailRequest.subject',
    domain: 'mailRequest.domain',
    senderEmail: 'mailRequest.senderEmail',
    recipientEmail: 'mailRequest.recipientEmail',
    templateSlug: 'mailRequest.templateSlug',
    mailType: convertCodesToMessage('mailRequest.mailTypeId', MAIL_TYPES),

    // Mail Report
    status: convertCodesToMessage('mailReport.eventId', STATUS_CODES),
    statusUpdatedAt: `STRING(TIMESTAMP_TRUNC(DATETIME(mailReport.createdAt,'${DEFAULT_TIMEZONE}'), SECOND))`,
    description: 'mailReport.reason',
};

class MailLogsService {
    private static instance: MailLogsService;

    public static getSingletonInstance(): MailLogsService {
        return MailLogsService.instance ||= new MailLogsService();
    }

    public getQuery(companyId: string, startDate: DateTime, endDate: DateTime, timeZone: string = DEFAULT_TIMEZONE, filters: { [key: string]: string } = {}, fields: string[] = []) {
        startDate = startDate.setZone(timeZone).set({ hour: 0, minute: 0, second: 0 });
        endDate = endDate.plus({ days: 1 }).setZone(timeZone).set({ hour: 0, minute: 0, second: 0 });
        const attributes = getValidFields(PERMITTED_FIELDS, fields).withAlias.join(',');
        const whereClause = this.getWhereClause(companyId, startDate, endDate, timeZone, filters);
        const query = `SELECT ${attributes}
            FROM ${MAIL_REQ_TABLE_ID} AS mailRequest 
            LEFT JOIN ${MAIL_REP_TABLE_ID} as mailReport 
            ON mailRequest.requestId = mailReport.requestId 
            WHERE ${whereClause}`;
        logger.info(query);
        return query;
    }

    private getWhereClause(companyId: string, startDate: DateTime, endDate: DateTime, timeZone: String, filters: { [key: string]: string }) {
        const query: { [key: string]: string } = filters;

        // mandatory conditions
        let conditions = `(mailRequest.createdAt BETWEEN "${startDate.setZone('utc').toFormat("yyyy-MM-dd HH:mm:ss z")}" AND "${endDate.setZone('utc').toFormat("yyyy-MM-dd HH:mm:ss z")}")`;
        conditions += ` AND (mailReport.requestTime BETWEEN "${startDate.setZone('utc').toFormat("yyyy-MM-dd HH:mm:ss z")}" AND "${endDate.setZone('utc').toFormat("yyyy-MM-dd HH:mm:ss z")}")`;

        // optional conditions
        if (query.subject) conditions += ` AND UPPER(mailRequest.subject) LIKE '%${query.subject.toUpperCase()}%'`;
        if (query.domain) conditions += ` AND mailRequest.domain in (${getQuotedStrings(query.domain.splitAndTrim(','))})`;
        if (query.senderEmail) conditions += ` AND UPPER(mailRequest.senderEmail) LIKE '%${query.senderEmail.toUpperCase()}%'`;
        if (query.recipientEmail) conditions += ` AND UPPER(mailRequest.recipientEmail) LIKE '%${query.recipientEmail.toUpperCase()}%'`;
        if (query.mailTypeId) conditions += ` AND mailRequest.mailTypeId in (${query.mailTypeId.splitAndTrim(',')})`;
        if (query.templateSlug) conditions += ` AND mailRequest.templateSlug in (${getQuotedStrings(query.templateSlug.splitAndTrim(','))})`;
        if (query.mailerRequestId) conditions += ` AND mailRequest.mailerRequestId in (${getQuotedStrings(query.mailerRequestId.splitAndTrim(','))})`;
        if (query.nodeId) conditions += ` AND mailRequest.nodeId in (${query.nodeId.splitAndTrim(',')})`;
        if (companyId) conditions += ` AND mailRequest.companyId = '${companyId}'`;

        if (companyId) conditions += ` AND mailReport.companyId = '${companyId}'`;
        if (query.senderDedicatedIPId) conditions += ` AND mailReport.senderDedicatedIPId in (${query.senderDedicatedIPId.splitAndTrim(',')})`;
        if (query.eventId) conditions += ` AND mailReport.eventId in (${query.eventId.splitAndTrim(',')})`;
        if (query.remoteMX) conditions += ` AND mailReport.remoteMX in (${getQuotedStrings(query.remoteMX.splitAndTrim(','))})`;
        if (query.remoteIP) conditions += ` AND mailReport.remoteIP in (${getQuotedStrings(query.remoteIP.splitAndTrim(','))})`;
        if (query.hostname) conditions += ` AND mailReport.hostname in (${getQuotedStrings(query.hostname.splitAndTrim(','))})`;

        return conditions;
    }
    public async getLogs(companyId: string, startDate: DateTime, endDate: DateTime, timeZone: string = DEFAULT_TIMEZONE, filters: { [key: string]: string } = {}, fields: string[] = [], option?: Option) {
        let query: string = this.getQuery(companyId, startDate, endDate, timeZone, filters, fields);
        if (option?.datasetId && option?.tableId) {
            // Set default values
            option.limit = option?.limit || 100;
            option.offset = option?.offset || 0;
            query = `SELECT * FROM \`${MSG91_PROJECT_ID}.${option?.datasetId}.${option?.tableId}\` LIMIT ${option.limit} OFFSET ${option?.offset}`;
        }
        let [data, metadata] = await getQueryResults(query, true);
        if (option?.limit) data = data?.slice(0, option?.limit);

        return {
            data, metadata: {
                ...metadata,
                tableId: option?.tableId || metadata?.tableId,
                datasetId: option?.datasetId || metadata?.datasetId,
            }
        };
    }
}

type Option = {
    datasetId?: string,
    tableId?: string,
    limit?: number,
    offset?: number
}
export default MailLogsService.getSingletonInstance();
