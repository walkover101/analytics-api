
import { getQueryResults, MSG91_PROJECT_ID, REPORT_DATA_TABLE_ID, REQUEST_DATA_TABLE_ID } from '../../database/big-query-service';
import { convertCodesToMessage, getQuotedStrings, getValidFields, prepareQuery, signToken, verifyToken } from '../utility-service';
import { DateTime } from 'luxon';
import logger from '../../logger/logger';
import { PaginationOption } from '../email/mail-logs-service';

const STATUS_CODES = {
    1: "Delivered",
    2: "Failed",
    3: "Delivered",
    5: "Pending",
    6: "Submitted",
    7: "Auto Failed",
    8: "Sent",
    9: "NDNC Number",
    13: "Failed",
    16: "Rejected By Provider",
    17: "Blocked Number",
    18: "Blocked Circle",
    19: "Price Range Blocked",
    20: "Country Code Blocked",
    25: "Rejected",
    26: "Delivered",
    28: "Invalid Number",
    29: "Invalid Number",
    30: "DLR not available",
    81: "Pending"
}
const BLOCKED_CREDITS = {
    9: 0,
    17: 0,
    18: 0,
    19: 0,
    20: 0
}
const DEFAULT_TIMEZONE: string = 'Asia/Kolkata';
const REQUEST_FIELDS: string[] = ['_id'].concat(['requestDate', 'campaign_name', 'scheduleDateTime', 'msgData', 'curRoute'].map(field => `ARRAY_AGG(${field} ORDER BY timestamp DESC)[OFFSET(0)] AS ${field}`));
const REPORT_FIELDS: string[] = ['status', 'sentTime', 'deliveryTime', 'telNum', 'senderID', 'requestId', 'failureReason', 'message', 'credit'].map(field => `ARRAY_AGG(${field} ORDER BY timestamp DESC)[OFFSET(0)] AS ${field}`);;
const PERMITTED_FIELDS: { [key: string]: string } = {
    // from report-data
    sentDateTime: `STRING(TIMESTAMP_TRUNC(DATETIME(reportData.sentTime,'${DEFAULT_TIMEZONE}'), SECOND))`,
    status: convertCodesToMessage('reportData.status', STATUS_CODES),
    deliveryDate: 'STRING(DATE(reportData.deliveryTime))',
    deliveryTime: 'STRING(TIME(reportData.deliveryTime))',
    requestId: 'reportData.requestID',
    telNum: 'reportData.telNum',
    credit: convertCodesToMessage('reportData.status', BLOCKED_CREDITS, false, 'reportData.credit'),
    senderId: 'reportData.senderID',
    failureReason: 'reportData.failureReason',

    // from request-data
    campaignName: 'requestData.campaign_name',
    scheduleDateTime: 'STRING(requestData.scheduleDateTime)',
    msgData: `CASE
    WHEN reportData.message IS NOT NULL
    THEN reportData.message
    ELSE requestData.msgData
    END
    `,
    route: 'requestData.curRoute'
};

class SmsLogsService {
    private static instance: SmsLogsService;


    public static getSingletonInstance(): SmsLogsService {
        return SmsLogsService.instance ||= new SmsLogsService();
    }

    public getQuery(companyId: string, startDate: DateTime, endDate: DateTime, timeZone: string = DEFAULT_TIMEZONE, filters: { [key: string]: string } = {}, fields: string[] = []) {
        startDate = startDate.setZone(timeZone).set({ hour: 0, minute: 0, second: 0 });
        endDate = endDate.plus({ days: 1 }).setZone(timeZone).set({ hour: 0, minute: 0, second: 0 });
        const attributes = getValidFields(PERMITTED_FIELDS, fields).withAlias.join(',');
        const reqWhereClause = this.getReqWhereClause(companyId, startDate, endDate, timeZone, filters);
        const repWhereClause = this.getRepWhereClause(companyId, startDate, endDate, timeZone, filters);
        const query = `SELECT ${attributes} 
            FROM (${prepareQuery(REQUEST_DATA_TABLE_ID, REQUEST_FIELDS, reqWhereClause, '_id')}) AS requestData
            JOIN (${prepareQuery(REPORT_DATA_TABLE_ID, REPORT_FIELDS, repWhereClause, '_id')}) AS reportData
            ON reportData.requestId = requestData._id`;
        logger.info(query);
        return query;
    }

    private getReqWhereClause(companyId: string, startDate: DateTime, endDate: DateTime, timeZone: String, filters: { [key: string]: string }) {
        const query: { [key: string]: string } = filters;

        // mandatory conditions
        let conditions = `(requestDate BETWEEN "${startDate.setZone('utc').toFormat("yyyy-MM-dd HH:mm:ss z")}" AND "${endDate.setZone('utc').toFormat("yyyy-MM-dd HH:mm:ss z")}")`;

        // optional conditions
        if (companyId) conditions += ` AND requestUserid = "${companyId}"`;
        if (query.route) conditions += ` AND curRoute in (${getQuotedStrings(query.route.splitAndTrim(','))})`;

        return conditions;
    }

    private getRepWhereClause(companyId: string, startDate: DateTime, endDate: DateTime, timeZone: String, filters: { [key: string]: string }) {
        const query: { [key: string]: string } = filters;

        // mandatory conditions
        let conditions = `(sentTime BETWEEN "${startDate.setZone('utc').toFormat("yyyy-MM-dd HH:mm:ss z")}" AND "${endDate.plus({ days: 1 }).setZone('utc').toFormat("yyyy-MM-dd HH:mm:ss z")}")`;

        // optional conditions
        if (companyId) conditions += `AND user_pid = "${companyId}"`;

        return conditions;
    }

    public async getLogs(companyId: string, startDate: DateTime, endDate: DateTime, timeZone: string = DEFAULT_TIMEZONE, filters: { [key: string]: string } = {}, fields: string[] = [], option?: PaginationOption) {
        let query: string = this.getQuery(companyId, startDate, endDate, timeZone, filters, fields);
        const pagination = (option?.paginationToken) ? verifyToken(option?.paginationToken) as PaginationOption : {};
        if (option && pagination?.datasetId && pagination?.tableId) {
            // Set default values
            option.limit = option?.limit || 100;
            option.offset = option?.offset || 0;
            query = `SELECT * FROM \`${MSG91_PROJECT_ID}.${pagination?.datasetId}.${pagination?.tableId}\` LIMIT ${option.limit} OFFSET ${option?.offset}`;
        }

        let [data, metadata] = await getQueryResults(query, true);
        if (option?.limit) data = data?.slice(0, option?.limit);

        return {
            data, metadata: {
                ...metadata,
                offset: option?.offset,
                limit: option?.limit,
                paginationToken: signToken({
                    tableId: pagination?.tableId || metadata?.tableId,
                    datasetId: pagination?.datasetId || metadata?.datasetId,
                })
            }
        };
    }
}

export default SmsLogsService.getSingletonInstance();
