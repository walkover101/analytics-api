import { getQueryResults, MSG91_PROJECT_ID, MSG91_DATASET_ID, MAIL_REQ_TABLE_ID, MAIL_REP_TABLE_ID } from '../../database/big-query-service';
import { DateTime } from 'luxon';
import logger from '../../logger/logger';
import { getValidFields, getQuotedStrings } from '../utility-service';

const DEFAULT_TIMEZONE: string = 'Asia/Kolkata';
const DEFAULT_GROUP_BY = 'date';
const PERMITTED_GROUPINGS: { [key: string]: string } = {
    // from request
    date: `STRING(DATE(request.createdAt,'${DEFAULT_TIMEZONE}'))`,
    nodeId: 'request.nodeId',
    reqId: 'request.mailerRequestId'
};

class MailAnalyticsService {
    private static instance: MailAnalyticsService;

    public static getSingletonInstance(): MailAnalyticsService {
        return MailAnalyticsService.instance ||= new MailAnalyticsService();
    }

    public async getAnalytics(companyId: string, startDate: DateTime, endDate: DateTime, timeZone: string = DEFAULT_TIMEZONE, filters: { [key: string]: string } = {}, groupBy?: string) {
        const query: string = this.getQuery(companyId, startDate, endDate, timeZone, filters, groupBy);
        const data = await getQueryResults(query);
        const total = this.calculateTotalAggr(data);
        return { data, total };
    }

    public getQuery(companyId: string, startDate: DateTime, endDate: DateTime, timeZone: string, filters: { [key: string]: string } = {}, groupings?: string) {
        if (filters.emailNodeIds?.length || filters.emailReqIds?.length) groupings = `nodeId,${groupings?.length ? groupings : 'date'}`;
        startDate = startDate.setZone(timeZone).set({ hour: 0, minute: 0, second: 0 });
        endDate = endDate.plus({ days: 1 }).setZone(timeZone).set({ hour: 0, minute: 0, second: 0 });
        const whereClause = this.getWhereClause(companyId, startDate, endDate, timeZone, filters);
        const validFields = getValidFields(PERMITTED_GROUPINGS, (groupings || DEFAULT_GROUP_BY).splitAndTrim(','));
        const groupBy = validFields.onlyAlias.join(',');
        const groupByAttribs = validFields.withAlias.join(',');
        const responseSubQuery = this.getResponseSubQuery(companyId, startDate, endDate, timeZone, filters);

        const query = `SELECT ${groupByAttribs}, ${this.aggregateAttribs()}
            FROM \`${MSG91_PROJECT_ID}.${MSG91_DATASET_ID}.${MAIL_REQ_TABLE_ID}\` AS request
            LEFT JOIN (${responseSubQuery}) AS response
            ON request.requestId = response.requestId
            WHERE ${whereClause}
            GROUP BY ${groupBy}
            ORDER BY ${groupBy}`;

        logger.info(query);
        return query;
    }

    private getResponseSubQuery(companyId: string, startDate: DateTime, endDate: DateTime, timeZone: string, filters: { [field: string]: string }) {
        return `SELECT
                    requestId,
                    ARRAY_REVERSE(ARRAY_AGG(eventId ORDER BY createdAt,eventId ASC))[OFFSET(0)] as eventId
                FROM \`${MSG91_PROJECT_ID}.${MSG91_DATASET_ID}.${MAIL_REP_TABLE_ID}\`
                WHERE
                    (requestTime BETWEEN "${startDate.setZone('utc').toFormat("yyyy-MM-dd HH:mm:ss z")}" AND "${endDate.setZone('utc').toFormat("yyyy-MM-dd HH:mm:ss z")}")
                    AND companyId = "${companyId}"
                GROUP BY requestId`;
    }

    private getWhereClause(companyId: string, startDate: DateTime, endDate: DateTime, timeZone: string, filters: { [field: string]: string }) {
        let conditions = `(request.createdAt BETWEEN "${startDate.setZone('utc').toFormat("yyyy-MM-dd HH:mm:ss z")}" AND "${endDate.setZone('utc').toFormat("yyyy-MM-dd HH:mm:ss z")}")`;
        conditions += ` AND request.companyId = "${companyId}"`;

        // optional conditions
        if (filters.emailNodeIds) conditions += ` AND request.nodeId in (${filters.emailNodeIds.splitAndTrim(',')})`;
        if (filters.emailReqIds) conditions += ` AND request.mailerRequestId in (${getQuotedStrings(filters.emailReqIds.splitAndTrim(','))})`;

        return conditions;
    }

    private aggregateAttribs() {
        return `COUNT(request.requestId) AS total,
            COUNTIF(eventId = 2) AS accepted,
            COUNTIF(eventId = 3) AS rejected,
            COUNTIF(eventId = 4) AS delivered,
            COUNTIF(eventId = 9) AS failed,
            COUNTIF(eventId = 8) AS bounced`;
    }

    private calculateTotalAggr(data: any) {
        const total = {
            "total": 0,
            "accepted": 0,
            "rejected": 0,
            "delivered": 0,
            "failed": 0,
            "bounced": 0
        }

        data.forEach((row: any) => {
            total["total"] += row["total"] || 0;
            total["accepted"] += row["accepted"] || 0;
            total["rejected"] += row["rejected"] || 0;
            total["delivered"] += row["delivered"] || 0;
            total["failed"] += row["failed"] || 0;
            total["bounced"] += row["bounced"] || 0;
        })

        return total;
    }
}

export default MailAnalyticsService.getSingletonInstance();
