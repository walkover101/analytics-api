import { getQuotedStrings, getValidFields, prepareQuery } from "../utility-service";
import { getQueryResults, VOICE_REP_TABLE_ID, VOICE_REQ_TABLE_ID, MSG91_DATASET_ID, MSG91_PROJECT_ID } from '../../database/big-query-service';
import { DateTime } from 'luxon';
import logger from '../../logger/logger';

const DEFAULT_TIMEZONE: string = 'Asia/Kolkata';
const DEFAULT_GROUP_BY = 'date';
const PERMITTED_GROUPINGS: { [key: string]: string } = {
    // from request-data
    date: `STRING(DATE(requestData.createdAt,'${DEFAULT_TIMEZONE}'))`
};

class VoiceAnalyticsService {
    private static instance: VoiceAnalyticsService;

    public static getSingletonInstance(): VoiceAnalyticsService {
        return VoiceAnalyticsService.instance ||= new VoiceAnalyticsService();
    }

    public async getAnalytics(companyId: string, startDate: DateTime, endDate: DateTime, timeZone: string = DEFAULT_TIMEZONE, filters: { [key: string]: string } = {}, groupBy?: string, onlyNodes: boolean = false) {
        const query: string = this.getQuery(companyId, startDate, endDate, timeZone, filters, groupBy, onlyNodes);
        const data = await getQueryResults(query);
        return { data };
    }

    public getQuery(companyId: string, startDate: DateTime, endDate: DateTime, timeZone: string = DEFAULT_TIMEZONE, filters: { [key: string]: string } = {}, groupings?: string, onlyNodes: boolean = false) {
        startDate = startDate.setZone(timeZone).set({ hour: 0, minute: 0, second: 0 });
        endDate = endDate.plus({ days: 1 }).setZone(timeZone).set({ hour: 0, minute: 0, second: 0 });

        const validFields = getValidFields(PERMITTED_GROUPINGS, (groupings || DEFAULT_GROUP_BY).splitAndTrim(','));
        const groupBy = validFields.onlyAlias.join(',');
        const groupByAttribs = validFields.withAlias.join(',');

        const query = `SELECT ${groupByAttribs},${this.aggregateAttribs()} FROM \`${MSG91_PROJECT_ID}.${MSG91_DATASET_ID}.${VOICE_REQ_TABLE_ID}\` as requestData
        LEFT JOIN \`${MSG91_PROJECT_ID}.${MSG91_DATASET_ID}.${VOICE_REP_TABLE_ID}\` as reportData
        ON requestData.uuid = reportData.uuid
        WHERE ${this.getWhereClause(companyId, startDate, endDate, DEFAULT_TIMEZONE, filters)}
        GROUP BY ${groupBy}
        ORDER BY ${groupBy}`;
        logger.info(query);
        return query;
    }

    private getWhereClause(companyId: string, startDate: DateTime, endDate: DateTime, timeZone: string, filters: { [field: string]: string }, onlyNodes: boolean = false) {
        // mandatory conditions
        let conditions = `(reportData.createdAt BETWEEN "${startDate.setZone('utc').toFormat("yyyy-MM-dd HH:mm:ss z")}" AND "${endDate.plus({ days: 1 }).setZone('utc').toFormat("yyyy-MM-dd HH:mm:ss z")}")`;
        conditions += ` AND (requestData.createdAt BETWEEN "${startDate.setZone('utc').toFormat("yyyy-MM-dd HH:mm:ss z")}" AND "${endDate.setZone('utc').toFormat("yyyy-MM-dd HH:mm:ss z")}")`;

        // optional conditions
        if (companyId) conditions += ` AND reportData.companyId = ${companyId} AND requestData.companyId = ${companyId}`;
        return conditions;
    }

    private aggregateAttribs() {
        return `COUNT(DISTINCT requestData.uuid) as total,
            COUNTIF(reportData.status = "completed") as completed,
            COUNTIF(reportData.status = "canceled") as canceled,
            ROUND(SUM(reportData.duration)) as duration,
            ROUND(SUM(reportData.billingDuration)) as billingDuration,
            ROUND(SUM(CAST(reportData.charged AS FLOAT64))) as charge`;
    }

}

export default VoiceAnalyticsService.getSingletonInstance();
