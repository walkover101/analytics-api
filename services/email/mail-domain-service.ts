import { getQueryResults, MSG91_PROJECT_ID, MSG91_DATASET_ID, MAIL_REQ_TABLE_ID, MAIL_REP_TABLE_ID, MAIL_EVENTS_TABLE_ID } from '../../database/big-query-service';
import { DateTime } from 'luxon';
import logger from '../../logger/logger';
import { getValidFields } from '../utility-service';

const DEFAULT_TIMEZONE: string = 'Asia/Kolkata';
const DEFAULT_GROUP_BY = 'date,domain,companyId';
const PERMITTED_GROUPINGS: { [key: string]: string } = {
    // from request
    date: `STRING(DATE(request.createdAt,'${DEFAULT_TIMEZONE}'))`,
    reqId: 'request.mailerRequestId',
    domain: 'request.domain',
    companyId: 'request.companyId'
};

class MailDomainService {
    private static instance: MailDomainService;

    public static getSingletonInstance(): MailDomainService {
        return MailDomainService.instance ||= new MailDomainService();
    }

    public async getAnalytics(companyId: string, startDate: DateTime, endDate: DateTime, timeZone: string) {
        const query: string = this.getQuery(companyId, startDate, endDate, timeZone);
        const data = await getQueryResults(query);
        const report = this.calculateTotalAggr(data);
        return { data, report };
    }

    public getQuery(companyId: string, startDate: DateTime, endDate: DateTime, timeZone: string) {
        startDate = startDate.setZone(timeZone).set({ hour: 0, minute: 0, second: 0 });
        endDate = endDate.plus({ days: 1 }).setZone(timeZone).set({ hour: 0, minute: 0, second: 0 });
        const whereClause = this.getWhereClause(companyId, startDate, endDate);
        const validFields = getValidFields(PERMITTED_GROUPINGS, DEFAULT_GROUP_BY.splitAndTrim(','));
        const groupBy = validFields.onlyAlias.join(',');
        const groupByAttribs = validFields.withAlias.join(',');
        const responseSubQuery = this.getResponseSubQuery(companyId, startDate, endDate);
        const eventSubQuery = this.getEventSubQuery(companyId, startDate, endDate);

        const query = `SELECT  ${this.aggregateAttribs()}
            FROM \`${MSG91_PROJECT_ID}.${MSG91_DATASET_ID}.${MAIL_REQ_TABLE_ID}\` AS request
            LEFT JOIN (${responseSubQuery}) AS response
            ON request.requestId = response.requestId
            JOIN (${eventSubQuery}) AS event
            ON response.requestId = event.requestId
            WHERE ${whereClause}
            GROUP BY ${groupBy}
            ORDER BY ${groupBy}`;

        logger.info(query);
        return query;
    }

    private getResponseSubQuery(companyId: string, startDate: DateTime, endDate: DateTime) {
        return `SELECT
                    requestId,
                    COUNTIF(eventId = 2) as accepted,
                    COUNTIF(eventId = 4) as delivered,
                    COUNTIF(eventId = 8) as bounced,
                    COUNTIF(eventId = 9) as failed,
                    COUNTIF(eventId = 3) as rejected,
                FROM \`${MSG91_PROJECT_ID}.${MSG91_DATASET_ID}.${MAIL_REP_TABLE_ID}\`
                WHERE
                    (requestTime BETWEEN "${startDate.setZone('utc').toFormat("yyyy-MM-dd HH:mm:ss z")}" AND "${endDate.setZone('utc').toFormat("yyyy-MM-dd HH:mm:ss z")}")
                    AND companyId = "${companyId}"
                GROUP BY requestId`;
    }

    private getEventSubQuery(companyId: string, startDate: DateTime, endDate: DateTime) {
        return `SELECT
                    requestId,
                    COUNTIF(eventId = 5) as opened,
                    COUNTIF(eventId = 6) as unsubscribed,
                    COUNTIF(eventId = 2) as complaint,
                FROM \`${MSG91_PROJECT_ID}.${MSG91_DATASET_ID}.${MAIL_EVENTS_TABLE_ID}\`
                WHERE
                    (requestTime BETWEEN "${startDate.setZone('utc').toFormat("yyyy-MM-dd HH:mm:ss z")}" AND "${endDate.setZone('utc').toFormat("yyyy-MM-dd HH:mm:ss z")}")
                    AND companyId = "${companyId}"
                GROUP BY requestId`;
    }

    private getWhereClause(companyId: string, startDate: DateTime, endDate: DateTime) {
        let conditions = `(request.createdAt BETWEEN "${startDate.setZone('utc').toFormat("yyyy-MM-dd HH:mm:ss z")}" AND "${endDate.setZone('utc').toFormat("yyyy-MM-dd HH:mm:ss z")}")`;

        // optional conditions
        if (companyId) conditions += ` AND request.companyId = "${companyId}"`;

        return conditions;
    }

    private aggregateAttribs() {
        return `date AS date,
        COUNT (DISTINCT requestId) AS totalRequest,
        COUNTIF(isSmtp = 1 OR (isSmtp = 0 AND eventId = 2)) AS Total,
        ANY_VALUE(companyId) AS companyId,
        ANY_VALUE(domain) AS domainName,
        COUNTIF(eventId = 4) as delivered,
        COUNTIF(eventId = 8) as bounced,
        COUNTIF(eventId = 9) as failed,
        COUNTIF(eventId = 3) as rejected,
        SUM(opened) AS opened,
        SUM(unsubscribed) AS unsubscribed,
        SUM(complaint) AS complaint`;
    }

    private calculateTotalAggr(data: any) {
        const report = {
            "total": 0,
            "delivered": 0,
            "bounced": 0,
            "failed": 0,
            "opened": 0,
            "unsubscribed": 0,
            "rejected": 0,
            "complaint": 0,
        }

        data.forEach((row: any) => {
            report["total"] += row["total"] || 0;
            report["delivered"] += row["delivered"] || 0;
            report["bounced"] += row["bounced"] || 0;
            report["failed"] += row["failed"] || 0;
            report["opened"] += row["opened"] || 0;
            report["unsubscribed"] += row["unsubscribed"] || 0;
            report["rejected"] += row["rejected"] || 0;
            report["complaint"] += row["complaint"] || 0;
        })

        return report;
    }
}

export default MailDomainService.getSingletonInstance();
