import { getQuotedStrings, getValidFields, prepareQuery } from "../utility-service";
import { getQueryResults, MSG91_DATASET_ID, MSG91_PROJECT_ID, OTP_TABLE_ID } from '../../database/big-query-service';
import { DateTime } from 'luxon';
import logger from '../../logger/logger';

const DEFAULT_TIMEZONE: string = 'Asia/Kolkata';
const DEFAULT_GROUP_BY = 'date';
const PERMITTED_GROUPINGS: { [key: string]: string } = {
    date: `STRING(DATE(otpData.requestDate,'${DEFAULT_TIMEZONE}'))`,
    country: 'otpData.countryCode',
    vendorId: 'otpData.smsc'
};
const REPORT_FIELDS: string[] = ['id'].concat(['countryCode', 'requestDate', 'deliveryTime', 'smsc', 'sentTime', 'status', 'reportStatus', 'credit'].map(field => `ARRAY_AGG(${field} ORDER BY timestamp DESC)[OFFSET(0)] AS ${field}`));

const DELIVERED_STATUS_CODES = [1, 3, 26];
const REJECTED_STATUS_CODES = [16, 17, 18, 19, 20, 29];
const FAILED_STATUS_CODES = [2, 5, 6, 7, 8, 13, 25, 28, 30, 81];
const NDNC_STATUS_CODES = [9];
// Don't add credit if request gets blocked or NDNC
const BLOCKED_STATUS_CODES = [17, 18, 19, 20];
const BLOCKED_AND_NDNC_CODES = BLOCKED_STATUS_CODES.concat(NDNC_STATUS_CODES);

class OtpAnalyticsService {
    private static instance: OtpAnalyticsService;

    public static getSingletonInstance(): OtpAnalyticsService {
        return OtpAnalyticsService.instance ||= new OtpAnalyticsService();
    }

    public async getAnalytics(companyId: string, startDate: DateTime, endDate: DateTime, timeZone: string = DEFAULT_TIMEZONE, filters: { [key: string]: string } = {}, groupBy?: string) {
        const query: string = this.getQuery(companyId, startDate, endDate, timeZone, filters, groupBy);
        const data = await getQueryResults(query);
        const total = this.calculateTotalAggr(data);
        return { data, total };
    }

    public getQuery(companyId: string, startDate: DateTime, endDate: DateTime, timeZone: string = DEFAULT_TIMEZONE, filters: { [key: string]: string } = {}, groupings?: string) {
        if (filters.vendorIds?.length) groupings = `vendorId,${groupings?.length ? groupings : 'date'}`;
        startDate = startDate.setZone(timeZone).set({ hour: 0, minute: 0, second: 0 });
        endDate = endDate.plus({ days: 1 }).setZone(timeZone).set({ hour: 0, minute: 0, second: 0 });
        const whereClause = this.getWhereClause(companyId, startDate, endDate, timeZone, filters);
        const validFields = getValidFields(PERMITTED_GROUPINGS, (groupings || DEFAULT_GROUP_BY).splitAndTrim(','));
        const groupBy = validFields.onlyAlias.join(',');
        const groupByAttribs = validFields.withAlias.join(',');
        const query = `SELECT ${groupByAttribs}, ${this.aggregateAttribs()}
            FROM (${prepareQuery(OTP_TABLE_ID, REPORT_FIELDS, whereClause, 'id')}) AS otpData
            GROUP BY ${groupBy}
            ORDER BY ${groupBy}`;

        logger.info(query);
        return query;
    }

    private getWhereClause(companyId: string, startDate: DateTime, endDate: DateTime, timeZone: string, filters: { [field: string]: string }) {
        // mandatory conditions
        let conditions = `(requestDate BETWEEN "${startDate.setZone('utc').toFormat("yyyy-MM-dd HH:mm:ss z")}" AND "${endDate.setZone('utc').toFormat("yyyy-MM-dd HH:mm:ss z")}")`;

        // optional conditions
        if (companyId) conditions += `AND requestUserid = "${companyId}"`;
        if (filters.vendorIds) conditions += `AND smsc in (${getQuotedStrings(filters.vendorIds.splitAndTrim(','))})`;

        return conditions;
    }

    private aggregateAttribs() {
        return `COUNT(otpData.id) as sent,
            ROUND(SUM(IF(otpData.reportStatus in (${BLOCKED_AND_NDNC_CODES.join(',')}), 0, otpData.credit)), 2) as balanceDeducted,
            ROUND(SUM(IF(otpData.reportStatus in (${DELIVERED_STATUS_CODES.join(',')}), otpData.credit,0)), 2) as deliveredCredit,
            ROUND(SUM(IF(otpData.reportStatus in (${FAILED_STATUS_CODES.join(',')}), otpData.credit,0)), 2) as failedCredit,
            ROUND(SUM(IF(otpData.reportStatus in (${REJECTED_STATUS_CODES.join(',')}), otpData.credit,0)), 2) as rejectedCredit,
            COUNTIF(otpData.reportStatus in (${DELIVERED_STATUS_CODES.join(',')})) as delivered,
            COUNTIF(otpData.reportStatus in (${FAILED_STATUS_CODES.join(',')})) as failed,
            COUNTIF(otpData.reportStatus in (${REJECTED_STATUS_CODES.join(',')})) as rejected,
            COUNTIF(otpData.reportStatus in (${NDNC_STATUS_CODES.join(',')})) as ndnc,
            ROUND(SUM(IF(otpData.reportStatus in (${DELIVERED_STATUS_CODES.join(',')}), TIMESTAMP_DIFF(otpData.deliveryTime, otpData.sentTime, SECOND), NULL))/COUNTIF(otpData.reportStatus in (${DELIVERED_STATUS_CODES.join(',')})), 0) as avgDeliveryTime`;
    }

    private calculateTotalAggr(data: any) {
        let totalDeliveryTime = 0;
        const total = {
            "message": 0,
            "delivered": 0,
            "totalCredits": 0,
            "failedCredits": 0,
            "rejectedCredits": 0,
            "deliveredCredits": 0,
            "filtered": 0,
            "avgDeliveryTime": 0
        }

        data.forEach((row: any) => {
            total["message"] += row["sent"] || 0;
            total["delivered"] += row["delivered"] || 0;
            total["totalCredits"] += row["balanceDeducted"] || 0;
            total["deliveredCredits"] += row["deliveredCredit"] || 0;
            total["failedCredits"] += row["failedCredit"] || 0;
            total["rejectedCredits"] += row["rejectedCredit"] || 0;
            totalDeliveryTime += row["avgDeliveryTime"] || 0;
        })

        total["filtered"] = total["message"] - total["delivered"];
        total["totalCredits"] = Number(total["totalCredits"].toFixed(3));
        total["deliveredCredits"] = Number(total["deliveredCredits"].toFixed(3));
        total["failedCredits"] = Number(total["failedCredits"].toFixed(3));
        total["rejectedCredits"] = Number(total["rejectedCredits"].toFixed(3));
        total["avgDeliveryTime"] = Number((totalDeliveryTime / data.length).toFixed(3));
        return total;
    }
}

export default OtpAnalyticsService.getSingletonInstance();
