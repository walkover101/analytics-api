import { getQuotedStrings, getValidFields } from "../utility-service";
import { getQueryResults } from '../../database/big-query-service';
import { DateTime } from 'luxon';
import logger from '../../logger/logger';

const DEFAULT_TIMEZONE: string = '+05:30';
const PROJECT_ID = process.env.GCP_PROJECT_ID;
const DATA_SET = process.env.MSG91_DATASET_ID;
const REQUEST_TABLE = process.env.REQUEST_DATA_TABLE_ID;
const REPORT_TABLE = process.env.REPORT_DATA_TABLE_ID;
const DEFAULT_GROUP_BY = 'date';
const PERMITTED_GROUPINGS: { [key: string]: string } = {
    // from report-data
    country: 'reportData.countryCode',
    vendorId: 'reportData.smsc',

    // from request-data
    date: 'STRING(DATE(requestData.requestDate))',
    nodeId: 'requestData.node_id'
};

class SmsService {
    private static instance: SmsService;

    public static getSingletonInstance(): SmsService {
        return SmsService.instance ||= new SmsService();
    }

    public async getAnalytics(companyId: string, startDate: DateTime, endDate: DateTime, timeZone: string = DEFAULT_TIMEZONE, filters: { [key: string]: string } = {}, groupBy?: string) {
        if (filters.smsNodeIds?.length) groupBy = `nodeId,${groupBy?.length ? groupBy : 'date'}`;
        if (filters.vendorIds?.length) groupBy = `vendorId,${groupBy?.length ? groupBy : 'date'}`;
        const query: string = this.getAnalyticsQuery(companyId, startDate, endDate, timeZone, filters, groupBy);
        const data = await getQueryResults(query);
        const total = this.calculateTotalAggr(data);
        return { data, total };
    }

    private getAnalyticsQuery(companyId: string, startDate: DateTime, endDate: DateTime, timeZone: string, filters: { [key: string]: string } = {}, groupings: string = DEFAULT_GROUP_BY) {
        const whereClause = this.getWhereClause(companyId, startDate, endDate, timeZone, filters);
        const validFields = getValidFields(PERMITTED_GROUPINGS, groupings.splitAndTrim(','));
        const groupBy = validFields.onlyAlias.join(',');
        const groupByAttribs = validFields.withAlias.join(',');

        const query = `SELECT ${groupByAttribs}, ${this.aggregateAttribs()}
            FROM \`${PROJECT_ID}.${DATA_SET}.${REPORT_TABLE}\` AS reportData
            INNER JOIN \`${PROJECT_ID}.${DATA_SET}.${REQUEST_TABLE}\` AS requestData
            ON reportData.requestID = requestData._id
            WHERE ${whereClause}
            GROUP BY ${groupBy}
            ORDER BY ${groupBy};`;

        logger.info(query);
        return query;
    }

    private getWhereClause(companyId: string, startDate: DateTime, endDate: DateTime, timeZone: string, filters: { [field: string]: string }) {
        // mandatory conditions
        let conditions = `(reportData.sentTime BETWEEN "${startDate.toFormat('yyyy-MM-dd')}" AND "${endDate.plus({ days: 3 }).toFormat('yyyy-MM-dd')}")`;
        conditions += ` AND (DATETIME(requestData.requestDate, '${timeZone}') BETWEEN DATETIME("${startDate.toFormat('yyyy-MM-dd')}", '${timeZone}') AND DATETIME("${endDate.toFormat('yyyy-MM-dd')}", '${timeZone}'))`;

        // optional conditions
        if (companyId) conditions += `AND reportData.user_pid = "${companyId}" AND requestData.requestUserid = "${companyId}"`;
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
            COUNTIF(reportData.status = 7) as autoFailed,
            ROUND(SUM(IF(reportData.status = 1, TIMESTAMP_DIFF(reportData.deliveryTime, reportData.sentTime, SECOND), NULL))/COUNTIF(reportData.status = 1), 0) as deliveryTime`;
    }

    private calculateTotalAggr(data: any) {
        let totalDeliveryTime = 0;
        const total = {
            "message": 0,
            "delivered": 0,
            "totalCredits": 0,
            "filtered": 0,
            "avgDeliveryTime": 0
        }

        data.forEach((row: any) => {
            total["message"] += row["sent"] || 0;
            total["delivered"] += row["delivered"] || 0;
            total["totalCredits"] += row["balanceDeducted"] || 0;
            totalDeliveryTime += row["deliveryTime"] || 0;
        })

        total["filtered"] = total["message"] - total["delivered"];
        total["totalCredits"] = Number(total["totalCredits"].toFixed(3));
        total["avgDeliveryTime"] = Number((totalDeliveryTime / data.length).toFixed(3));
        return total;
    }
}

export default SmsService.getSingletonInstance();
