import { getQueryResults, MSG91_PROJECT_ID, MSG91_DATASET_ID, MAIL_REQ_TABLE_ID, MAIL_REP_TABLE_ID, MAIL_EVENTS_TABLE_ID } from '../../database/big-query-service';
import { DateTime } from 'luxon';
import logger from '../../logger/logger';
import { getValidFields } from '../utility-service';

const attr = 'requestId, domain, companyId, eventId, isSmtp, opened, unsubscribed, complaint';
const DEFAULT_GROUP_BY = 'domain';
const PERMITTED_GROUPINGS: { [key: string]: string } = {
    // from request
    domain: 'domain',
};

class MailDomainService {
    private static instance: MailDomainService;

    public static getSingletonInstance(): MailDomainService {
        return MailDomainService.instance ||= new MailDomainService();
    }

    public async getAnalytics(companyId: string, startDate: DateTime, endDate: DateTime, timeZone: string) {
        const query: string = this.getQuery(companyId, startDate, endDate, timeZone);
        const data = await getQueryResults(query);
        const result = this.calculateTotalAggr(data);
        return {result};
    }

    public getQuery(companyId: string, startDate: DateTime, endDate: DateTime, timeZone: string) {
        startDate = startDate.setZone(timeZone).set({ hour: 0, minute: 0, second: 0 });
        endDate = endDate.plus({ days: 1 }).setZone(timeZone).set({ hour: 0, minute: 0, second: 0 });
        const whereClause = this.getWhereClause(companyId, startDate, endDate);
        const validFields = getValidFields(PERMITTED_GROUPINGS, DEFAULT_GROUP_BY.splitAndTrim(','));
        const groupBy = validFields.onlyAlias.join(',');
        const subQuery = this.getSubQuery(companyId, startDate, endDate);

        const query = `SELECT  ${this.aggregateAttribs()}
            FROM (
            SELECT ${attr}
            FROM \`${MSG91_PROJECT_ID}.${MSG91_DATASET_ID}.${MAIL_REQ_TABLE_ID}\` AS request
            LEFT JOIN (${subQuery})
            ON request.requestId = reportId
            WHERE ${whereClause}
            )
            GROUP BY ${groupBy};
           `;
        logger.info(query);
        return query;
    }

    private getSubQuery(companyId: string, startDate: DateTime, endDate: DateTime) {
        return `SELECT
        report.requestId AS reportId,
        COUNTIF(event.eventId = 5) as opened,
        COUNTIF(event.eventId = 6) as unsubscribed,
        COUNTIF(event.eventId = 10) as complaint,
        ARRAY_REVERSE(ARRAY_AGG(report.eventId ORDER BY report.requestTime,report.eventId ASC))[OFFSET(0)] as eventId
        FROM \`${MSG91_PROJECT_ID}.${MSG91_DATASET_ID}.${MAIL_REP_TABLE_ID}\` AS report
            LEFT JOIN \`${MSG91_PROJECT_ID}.${MSG91_DATASET_ID}.${MAIL_EVENTS_TABLE_ID}\` AS event
            ON report.requestId = event.requestId
            WHERE (
            report.requestTime BETWEEN "${startDate.setZone('utc').toFormat("yyyy-MM-dd HH:mm:ss z")}" AND "${endDate.setZone('utc').toFormat("yyyy-MM-dd HH:mm:ss z")}")
            AND report.companyId = "${companyId}"
            GROUP BY report.requestId`
    }

    private getWhereClause(companyId: string, startDate: DateTime, endDate: DateTime) {
        let conditions = `(createdAt BETWEEN "${startDate.setZone('utc').toFormat("yyyy-MM-dd HH:mm:ss z")}" AND "${endDate.setZone('utc').toFormat("yyyy-MM-dd HH:mm:ss z")}")`;

        // optional conditions
        if (companyId) conditions += ` AND companyId = "${companyId}"`;

        return conditions;
    }

    private aggregateAttribs() {
        return `
        COUNT (DISTINCT requestId) AS totalRequest,
        COUNTIF(isSmtp = 1 OR (isSmtp = 0 AND eventId IS NOT NULL)) AS Total,
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
        let companyId: string = '';
        let domain: string = '';
        let report = {
            "total": 0,
            "delivered": 0,
            "bounced": 0,
            "failed": 0,
            "opened": 0,
            "unsubscribed": 0,
            "rejected": 0,
            "complaint": 0,
        };
        let result: any = [];

        data.forEach((row: any) => {
            companyId = row["companyId"];
            domain = row["domainName"];
            report["total"] = row["Total"] || 0;
            report["delivered"] = row["delivered"] || 0;
            report["bounced"] = row["bounced"] || 0;
            report["failed"] = row["failed"] || 0;
            report["opened"] = row["opened"] || 0;
            report["unsubscribed"] = row["unsubscribed"] || 0;
            report["rejected"] = row["rejected"] || 0;
            report["complaint"] = row["complaint"] || 0;
            result.push({companyId,domain,report})
        })

        return result;
    }
}

export default MailDomainService.getSingletonInstance();
