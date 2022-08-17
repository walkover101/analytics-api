import { getQueryResults } from '../../database/big-query-service';
import { DateTime } from 'luxon';
import logger from '../../logger/logger';

const DEFAULT_TIMEZONE: string = '+05:30';
const DEFAULT_GROUP_BY = 'date';

class SmsAnalyticsService {
    private static instance: SmsAnalyticsService;

    public static getSingletonInstance(): SmsAnalyticsService {
        return SmsAnalyticsService.instance ||= new SmsAnalyticsService();
    }

    public async getAnalytics(companyId: string, startDate: DateTime, endDate: DateTime, timeZone: string = DEFAULT_TIMEZONE, filters: { [key: string]: string } = {}, groupBy?: string) {
        const query: string = this.getAnalyticsQuery(companyId, startDate, endDate, timeZone, filters, groupBy);
        const data = await getQueryResults(query);
        return { data };
    }

    private getAnalyticsQuery(companyId: string, startDate: DateTime, endDate: DateTime, timeZone: string, filters: { [key: string]: string } = {}, groupings: string = DEFAULT_GROUP_BY) {
        const query = `SELECT 
            DATE(request.createdAt) AS date,
            COUNT(request.requestId) AS total,
            COUNTIF(eventId = 2) AS accepted,
            COUNTIF(eventId = 3) AS rejected,
            COUNTIF(eventId = 4) AS delivered,
            COUNTIF(eventId = 9) AS failed,
            COUNTIF(eventId = 8) AS bounced
        FROM \`msg91-reports.msg91_test.mail_request\` as request
        JOIN (
            SELECT requestId, ARRAY_AGG(eventId ORDER BY createdAt DESC)[OFFSET(0)] as eventId FROM \`msg91-reports.msg91_test.mail_report\` 
            WHERE (requestTime BETWEEN "${startDate}" AND "${endDate}") AND companyId = "${companyId}"
            GROUP BY requestId
            ) AS response
        ON request.requestId = response.requestId
        WHERE request.createdAt BETWEEN "${endDate}" AND "${endDate}" AND companyId = "${companyId}"
        GROUP BY date
        ORDER BY date;`;

        logger.info(query);
        return query;
    }
}

export default SmsAnalyticsService.getSingletonInstance();
