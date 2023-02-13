import { getQuotedStrings, getValidFields, prepareQuery } from "../utility-service";
import { getQueryResults, REPORT_DATA_TABLE_ID, REQUEST_DATA_TABLE_ID } from '../../database/big-query-service';
import { DateTime } from 'luxon';
import logger from '../../logger/logger';

const DEFAULT_TIMEZONE: string = 'Asia/Kolkata';
const DEFAULT_GROUP_BY = 'date';
const PERMITTED_GROUPINGS: { [key: string]: string } = {
    // from request-data
    date: `STRING(DATE(requestData.requestDate,'${DEFAULT_TIMEZONE}'))`,
    nodeId: 'requestData.node_id',
    plugin: 'requestData.plugin',

    // from report-data
    country: 'reportData.countryCode',
    vendorId: 'reportData.smsc',
    reqId: 'reportData.requestID'
};
const REQUEST_FIELDS: string[] = ['_id'].concat(['requestDate', 'node_id', 'plugin'].map(field => `ARRAY_AGG(${field} ORDER BY timestamp DESC)[OFFSET(0)] AS ${field}`));
const REPORT_FIELDS: string[] = ['countryCode', 'smsc', 'requestID', 'status', 'credit'].map(field => `ARRAY_AGG(${field} ORDER BY timestamp DESC)[OFFSET(0)] AS ${field}`);;
const DELIVERED_STATUS_CODES = [1, 3, 26];
const REJECTED_STATUS_CODES = [16, 17, 18, 19, 20, 29];
const FAILED_STATUS_CODES = [2, 5, 6, 7, 8, 13, 25, 28, 30, 81];
const NDNC_STATUS_CODES = [9];
// Don't add credit if request gets blocked or NDNC
const BLOCKED_STATUS_CODES = [17, 18, 19, 20];
const BLOCKED_AND_NDNC_CODES = BLOCKED_STATUS_CODES.concat(NDNC_STATUS_CODES);

class SmsAnalyticsService {
    private static instance: SmsAnalyticsService;

    public static getSingletonInstance(): SmsAnalyticsService {
        return SmsAnalyticsService.instance ||= new SmsAnalyticsService();
    }

    public async getAnalytics(companyId: string, startDate: DateTime, endDate: DateTime, timeZone: string = DEFAULT_TIMEZONE, filters: { [key: string]: string } = {}, groupBy?: string, onlyNodes: boolean = false) {
        const query: string = this.getQuery(companyId, startDate, endDate, timeZone, filters, groupBy, onlyNodes);
        const data = await getQueryResults(query);
        const total = this.calculateTotalAggr(data);
        return { data, total };
    }

    public getQuery(companyId: string, startDate: DateTime, endDate: DateTime, timeZone: string = DEFAULT_TIMEZONE, filters: { [key: string]: string } = {}, groupings?: string, onlyNodes: boolean = false) {
        if (filters.smsNodeIds?.length || filters.smsReqIds?.length) groupings = `nodeId,${groupings?.length ? groupings : 'date'}`;
        if (filters.vendorIds?.length) groupings = `vendorId,${groupings?.length ? groupings : 'date'}`;
        startDate = startDate.setZone(timeZone).set({ hour: 0, minute: 0, second: 0 });
        endDate = endDate.plus({ days: 1 }).setZone(timeZone).set({ hour: 0, minute: 0, second: 0 });
        const repWhereClause = this.getRepWhereClause(companyId, startDate, endDate, timeZone, filters, onlyNodes);
        const reqWhereClause = this.getReqWhereClause(companyId, startDate, endDate, timeZone, filters, onlyNodes);
        const validFields = getValidFields(PERMITTED_GROUPINGS, (groupings || DEFAULT_GROUP_BY).splitAndTrim(','));
        const groupBy = validFields.onlyAlias.join(',');
        const groupByAttribs = validFields.withAlias.join(',');

        const query = `SELECT ${groupByAttribs}, ${this.aggregateAttribs()}
            FROM (${prepareQuery(REQUEST_DATA_TABLE_ID, REQUEST_FIELDS, reqWhereClause, '_id')}) AS requestData
            LEFT JOIN (${prepareQuery(REPORT_DATA_TABLE_ID, REPORT_FIELDS, repWhereClause, '_id')}) AS reportData
            ON reportData.requestID = requestData._id
            GROUP BY ${groupBy}
            ORDER BY ${groupBy}`;

        logger.info(query);
        return query;
    }

    private getRepWhereClause(companyId: string, startDate: DateTime, endDate: DateTime, timeZone: string, filters: { [field: string]: string }, onlyNodes: boolean = false) {
        // mandatory conditions
        let conditions = `(sentTime BETWEEN "${startDate.setZone('utc').toFormat("yyyy-MM-dd HH:mm:ss z")}" AND "${endDate.plus({ days: 1 }).setZone('utc').toFormat("yyyy-MM-dd HH:mm:ss z")}")`;

        // optional conditions
        if (companyId) conditions += ` AND user_pid = "${companyId}"`;
        if (filters.vendorIds) conditions += `AND smsc in (${getQuotedStrings(filters.vendorIds.splitAndTrim(','))})`;
        if (filters.smsReqIds) conditions += ` AND requestID in (${getQuotedStrings(filters.smsReqIds.splitAndTrim(','))})`;

        return conditions;
    }

    private getReqWhereClause(companyId: string, startDate: DateTime, endDate: DateTime, timeZone: string, filters: { [field: string]: string }, onlyNodes: boolean = false) {
        // mandatory conditions
        let conditions = `(requestDate BETWEEN "${startDate.setZone('utc').toFormat("yyyy-MM-dd HH:mm:ss z")}" AND "${endDate.setZone('utc').toFormat("yyyy-MM-dd HH:mm:ss z")}")`;

        // optional conditions
        if (companyId) conditions += ` AND requestUserid = "${companyId}"`;
        if (filters.campaignId) conditions += ` AND campaign_pid in (${getQuotedStrings(filters.campaignId.splitAndTrim(','))})`;
        if (filters.campaignName) conditions += ` AND campaign_name in (${getQuotedStrings(filters.campaignName.splitAndTrim(','))})`;
        if (filters.route) conditions += ` AND curRoute in (${getQuotedStrings(filters.route.splitAndTrim(','))})`;
        if (filters.smsNodeIds) conditions += ` AND node_id in (${getQuotedStrings(filters.smsNodeIds.splitAndTrim(','))})`;
        if (onlyNodes) conditions += ` AND node_id IS NOT NULL`;

        return conditions;
    }

    private aggregateAttribs() {
        return `COUNT(reportData.requestID) as sent,
            ROUND(SUM(IF(reportData.status in (${BLOCKED_AND_NDNC_CODES.join(',')}), 0, reportData.credit)), 2) as balanceDeducted,
            ROUND(SUM(IF(reportData.status in (${DELIVERED_STATUS_CODES.join(',')}), reportData.credit, 0)), 2) as deliveredCredit,
            ROUND(SUM(IF(reportData.status in (${FAILED_STATUS_CODES.join(',')}), reportData.credit, 0)), 2) as failedCredit,
            ROUND(SUM(IF(reportData.status in (${REJECTED_STATUS_CODES.join(',')}), reportData.credit, 0)), 2) as rejectedCredit,
            COUNTIF(reportData.status in (${DELIVERED_STATUS_CODES.join(',')})) as delivered,
            COUNTIF(reportData.status in (${FAILED_STATUS_CODES.join(',')})) as failed,
            COUNTIF(reportData.status in (${REJECTED_STATUS_CODES.join(',')})) as rejected,
            COUNTIF(reportData.status in (${NDNC_STATUS_CODES.join(',')})) as ndnc`;
    }

    private calculateTotalAggr(data: any) {
        const total = {
            "message": 0,
            "delivered": 0,
            "failed": 0,
            "rejected": 0,
            "ndnc": 0,
            "totalCredits": 0,
            "failedCredits": 0,
            "rejectedCredits": 0,
            "deliveredCredits": 0,
            "filtered": 0
        }

        data.forEach((row: any) => {
            total["message"] += row["sent"] || 0;
            total["delivered"] += row["delivered"] || 0;
            total["failed"] += row["failed"] || 0;
            total["rejected"] += row["rejected"] || 0;
            total["ndnc"] += row["ndnc"] || 0;
            total["totalCredits"] += row["balanceDeducted"] || 0;
            total["deliveredCredits"] += row["deliveredCredit"] || 0;
            total["failedCredits"] += row["failedCredit"] || 0;
            total["rejectedCredits"] += row["rejectedCredit"] || 0;
        })

        total["filtered"] = total["message"] - total["delivered"];
        total["totalCredits"] = Number(total["totalCredits"].toFixed(3));
        total["deliveredCredits"] = Number(total["deliveredCredits"].toFixed(3));
        total["failedCredits"] = Number(total["failedCredits"].toFixed(3));
        total["rejectedCredits"] = Number(total["rejectedCredits"].toFixed(3));

        return total;
    }
}

export default SmsAnalyticsService.getSingletonInstance();
