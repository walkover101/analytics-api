import { getQuotedStrings, getValidFields } from "../utility-service";
import { getQueryResults, MSG91_DATASET_ID, MSG91_PROJECT_ID, REPORT_DATA_TABLE_ID, REQUEST_DATA_TABLE_ID } from '../../database/big-query-service';
import { DateTime } from 'luxon';
import logger from '../../logger/logger';

const DEFAULT_TIMEZONE: string = 'Asia/Kolkata';
const DEFAULT_GROUP_BY = 'date';
const PERMITTED_GROUPINGS: { [key: string]: string } = {
    // from report-data
    country: 'reportData.countryCode',
    vendorId: 'reportData.smsc',

    // from request-data
    date: `STRING(DATE(requestData.requestDate,'${DEFAULT_TIMEZONE}'))`,
    nodeId: 'requestData.node_id'
};

class SmsAnalyticsService {
    private static instance: SmsAnalyticsService;

    public static getSingletonInstance(): SmsAnalyticsService {
        return SmsAnalyticsService.instance ||= new SmsAnalyticsService();
    }

    public async getAnalytics(companyId: string, startDate: DateTime, endDate: DateTime, timeZone: string = DEFAULT_TIMEZONE, filters: { [key: string]: string } = {}, groupBy?: string) {
        const query: string = this.getQuery(companyId, startDate, endDate, timeZone, filters, groupBy);
        const data = await getQueryResults(query);
        const total = this.calculateTotalAggr(data);
        return { data, total };
    }

    public getQuery(companyId: string, startDate: DateTime, endDate: DateTime, timeZone: string = DEFAULT_TIMEZONE, filters: { [key: string]: string } = {}, groupings?: string) {
        if (filters.smsNodeIds?.length) groupings = `nodeId,${groupings?.length ? groupings : 'date'}`;
        if (filters.vendorIds?.length) groupings = `vendorId,${groupings?.length ? groupings : 'date'}`;
        startDate = startDate.setZone(timeZone).set({ hour: 0, minute: 0, second: 0 });
        endDate = endDate.plus({ days: 1 }).setZone(timeZone).set({ hour: 0, minute: 0, second: 0 });
        const whereClause = this.getWhereClause(companyId, startDate, endDate, timeZone, filters);
        const validFields = getValidFields(PERMITTED_GROUPINGS, (groupings || DEFAULT_GROUP_BY).splitAndTrim(','));
        const groupBy = validFields.onlyAlias.join(',');
        const groupByAttribs = validFields.withAlias.join(',');

        const query = `SELECT ${groupByAttribs}, ${this.aggregateAttribs()}
            FROM \`${MSG91_PROJECT_ID}.${MSG91_DATASET_ID}.${REPORT_DATA_TABLE_ID}\` AS reportData
            LEFT JOIN \`${MSG91_PROJECT_ID}.${MSG91_DATASET_ID}.${REQUEST_DATA_TABLE_ID}\` AS requestData
            ON reportData.requestID = requestData._id
            WHERE ${whereClause}
            GROUP BY ${groupBy}
            ORDER BY ${groupBy}`;

        logger.info(query);
        return query;
    }


    private getWhereClause(companyId: string, startDate: DateTime, endDate: DateTime, timeZone: string, filters: { [field: string]: string }) {
        // mandatory conditions
        let conditions = `(reportData.sentTime BETWEEN "${startDate.setZone('utc').toFormat("yyyy-MM-dd HH:mm:ss z")}" AND "${endDate.plus({ days: 1 }).setZone('utc').toFormat("yyyy-MM-dd HH:mm:ss z")}")`;
        conditions += ` AND (requestData.requestDate BETWEEN "${startDate.setZone('utc').toFormat("yyyy-MM-dd HH:mm:ss z")}" AND "${endDate.setZone('utc').toFormat("yyyy-MM-dd HH:mm:ss z")}")`;

        // optional conditions
        if (companyId) conditions += `AND reportData.user_pid = "${companyId}" AND requestData.requestUserid = "${companyId}"`;
        if (filters.campaignId) conditions += ` AND requestData.campaign_pid in (${getQuotedStrings(filters.campaignId.splitAndTrim(','))})`;
        if (filters.campaignName) conditions += ` AND requestData.campaign_name in (${getQuotedStrings(filters.campaignName.splitAndTrim(','))})`;
        if (filters.vendorIds) conditions += `AND reportData.smsc in (${getQuotedStrings(filters.vendorIds.splitAndTrim(','))})`;
        if (filters.route) conditions += ` AND requestData.curRoute in (${getQuotedStrings(filters.route.splitAndTrim(','))})`;
        if (filters.smsNodeIds) conditions += ` AND requestData.node_id in (${getQuotedStrings(filters.smsNodeIds.splitAndTrim(','))})`;

        return conditions;
    }

    private aggregateAttribs() {
        // Don't add credit if request gets blocked or NDNC

        return `COUNT(reportData._id) as sent,
            ROUND(SUM(IF(reportData.status in (17, 9), 0, reportData.credit)), 2) as balanceDeducted,
            COUNTIF(reportData.status in (1, 3, 26)) as delivered,
            COUNTIF(reportData.status in (2, 13, 7)) as failed,
            COUNTIF(reportData.status in (25, 16)) as rejected,
            COUNTIF(reportData.status = 9) as ndnc,
            COUNTIF(reportData.status = 17) as blocked,
            COUNTIF(reportData.status = 7) as autoFailed`;
    }

    private calculateTotalAggr(data: any) {
        const total = {
            "message": 0,
            "delivered": 0,
            "totalCredits": 0,
            "filtered": 0
        }

        data.forEach((row: any) => {
            total["message"] += row["sent"] || 0;
            total["delivered"] += row["delivered"] || 0;
            total["totalCredits"] += row["balanceDeducted"] || 0;
        })

        total["filtered"] = total["message"] - total["delivered"];
        total["totalCredits"] = Number(total["totalCredits"].toFixed(3));
        
        return total;
    }
}

export default SmsAnalyticsService.getSingletonInstance();
