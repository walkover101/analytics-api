import { getValidFields } from "../utility-service";
import { getQueryResults, MSG91_DATASET_ID, MSG91_PROJECT_ID, WA_REQ_TABLE_ID, WA_REP_TABLE_ID } from '../../database/big-query-service';
import { DateTime } from 'luxon';
import logger from '../../logger/logger';

const DEFAULT_TIMEZONE: string = 'Asia/Kolkata';
const DEFAULT_GROUP_BY = 'date';
const PERMITTED_GROUPINGS: { [key: string]: string } = {
    // from request-data
    date: `STRING(DATE(requestData.timestamp,'${DEFAULT_TIMEZONE}'))`,
    nodeId: 'requestData.nodeId',
    microservice: '(SELECT "WHATSAPP")'
};

class WaAnalyticsService {
    private static instance: WaAnalyticsService;

    public static getSingletonInstance(): WaAnalyticsService {
        return WaAnalyticsService.instance ||= new WaAnalyticsService();
    }

    public async getAnalytics(companyId: string, startDate: DateTime, endDate: DateTime, timeZone: string = DEFAULT_TIMEZONE, filters: { [key: string]: string } = {}, groupBy?: string) {
        const query: string = this.getQuery(companyId, startDate, endDate, timeZone, filters, groupBy);
        const data = await getQueryResults(query);
        const total = this.calculateTotalAggr(data);
        return { data, total };
    }

    public getQuery(companyId: string, startDate: DateTime, endDate: DateTime, timeZone: string = DEFAULT_TIMEZONE, filters: { [key: string]: string } = {}, groupings?: string) {
        if (filters.waNodeIds?.length) groupings = `nodeId,${groupings?.length ? groupings : 'date'}`;
        startDate = startDate.setZone(timeZone).set({ hour: 0, minute: 0, second: 0 });
        endDate = endDate.plus({ days: 1 }).setZone(timeZone).set({ hour: 0, minute: 0, second: 0 });
        const whereClause = this.getWhereClause(companyId, startDate, endDate, filters, groupings);
        const validFields = getValidFields(PERMITTED_GROUPINGS, (groupings || DEFAULT_GROUP_BY).splitAndTrim(','));
        const groupBy = validFields.onlyAlias.join(',');
        const groupByAttribs = validFields.withAlias.join(',');
        const responseSubQuery = this.getResponseSubQuery(companyId, startDate, endDate, filters);

        const query = `SELECT ${groupByAttribs}, ${this.aggregateAttribs()}
            FROM \`${MSG91_PROJECT_ID}.${MSG91_DATASET_ID}.${WA_REQ_TABLE_ID}\` AS requestData
            JOIN (${responseSubQuery}) AS reportData
            ON reportData.uuid = requestData.uuid
            WHERE ${whereClause}
            GROUP BY ${groupBy}
            ORDER BY ${groupBy}`;

        logger.info(query);
        return query;
    }

    private getResponseSubQuery(companyId: string, startDate: DateTime, endDate: DateTime, filters: { [field: string]: string }) {
        return `SELECT uuid, ARRAY_AGG(status ORDER BY timestamp DESC)[OFFSET(0)] AS status,
                ARRAY_AGG(timestamp ORDER BY timestamp ASC)[OFFSET(0)] AS sentTime
                FROM \`${MSG91_PROJECT_ID}.${MSG91_DATASET_ID}.${WA_REP_TABLE_ID}\` AS report
                WHERE (submittedAt BETWEEN "${startDate.setZone('utc').toFormat("yyyy-MM-dd HH:mm:ss z")}" AND "${endDate.setZone('utc').toFormat("yyyy-MM-dd HH:mm:ss z")}")
                AND companyId = "${companyId}"
                GROUP BY uuid`;
    }

    private getWhereClause(companyId: string, startDate: DateTime, endDate: DateTime, filters: { [field: string]: string }, groupings?: string) {
        // mandatory conditions
        let conditions = `(requestData.timestamp BETWEEN "${startDate.setZone('utc').toFormat("yyyy-MM-dd HH:mm:ss z")}" AND "${endDate.setZone('utc').toFormat("yyyy-MM-dd HH:mm:ss z")}")`;

        // optional conditions
        if (companyId) conditions += ` AND requestData.companyId = "${companyId}"`;

        if (groupings === 'microservice') {
            conditions += ` AND requestData.nodeId is NOT NULL`;
        } else {
            if (filters.waNodeIds) conditions += ` AND requestData.nodeId in (${filters.waNodeIds.splitAndTrim(',')})`;
        }

        return conditions;
    }

    private aggregateAttribs() {
        return `COUNT(requestData.uuid) AS total,
            COUNTIF(reportData.status = "sent") AS sent,
            COUNTIF(reportData.status = "delivered") AS delivered,
            COUNTIF(reportData.status = "read") AS read,
            TIMESTAMP_DIFF(ANY_VALUE(reportData.sentTime,ANY_VALUE(requestData.timestamp)), SECOND) AS avgDeliveryTime`;
    }

    private calculateTotalAggr(data: any) {
        let avgDeliveryTime = 0;

        const total = {
            "total": 0,
            "sent": 0,
            "delivered": 0,
            "read": 0,
            "avgDeliveryTime": 0
        }

        data.forEach((row: any) => {
            total["total"] += row["total"] || 0;
            total["sent"] += row["sent"] || 0;
            total["delivered"] += row["delivered"] || 0;
            total["read"] += row["read"] || 0;
            avgDeliveryTime += row["avgDeliveryTime"] || 0;
        });

        total["avgDeliveryTime"] = Number((avgDeliveryTime / data.length).toFixed(3)) || 0;
        return total;
    }
}

export default WaAnalyticsService.getSingletonInstance();
