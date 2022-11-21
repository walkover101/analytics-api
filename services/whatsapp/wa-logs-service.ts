
import { getQueryResults, MSG91_DATASET_ID, MSG91_PROJECT_ID, WA_REQ_TABLE_ID, WA_REP_TABLE_ID } from '../../database/big-query-service';
import { getQuotedStrings, getValidFields } from '../utility-service';
import { DateTime } from 'luxon';
import logger from '../../logger/logger';

const DEFAULT_TIMEZONE: string = 'Asia/Kolkata';
const PERMITTED_FIELDS: { [key: string]: string } = {
    // from report-data
    submittedAt: 'STRING(reportData.submittedAt)',
    price: 'reportData.price',
    origin: 'reportData.origin',
    reportStatus: 'reportData.status',
    reason: 'reportData.reason',

    // from request-data
    uuid: 'requestData.uuid',
    integratedNumber: 'requestData.integratedNumber',
    customerNumber: 'requestData.customerNumber',
    vendorId: 'requestData.vendorId',
    messageType: 'requestData.messageType',
    direction: 'requestData.direction',
    timestamp: 'STRING(reportData.timestamp)',
    content: 'requestData.content',
    requestStatus: 'requestData.status'
};

class WaLogsService {
    private static instance: WaLogsService;


    public static getSingletonInstance(): WaLogsService {
        return WaLogsService.instance ||= new WaLogsService();
    }

    public getQuery(companyId: string, startDate: DateTime, endDate: DateTime, timeZone: string = DEFAULT_TIMEZONE, filters: { [key: string]: string } = {}, fields: string[] = []) {
        startDate = startDate.setZone(timeZone).set({ hour: 0, minute: 0, second: 0 });
        endDate = endDate.plus({ days: 1 }).setZone(timeZone).set({ hour: 0, minute: 0, second: 0 });
        const attributes = getValidFields(PERMITTED_FIELDS, fields).withAlias.join(',');
        const whereClause = this.getWhereClause(companyId, startDate, endDate, timeZone, filters);
        const query = `SELECT ${attributes} 
            FROM \`${MSG91_PROJECT_ID}.${MSG91_DATASET_ID}.${WA_REQ_TABLE_ID}\` as requestData
            LEFT JOIN \`${MSG91_PROJECT_ID}.${MSG91_DATASET_ID}.${WA_REP_TABLE_ID}\` as reportData
            ON requestData.uuid = reportData.uuid
            WHERE ${whereClause}`;
        logger.info(query);
        return query;
    }

    private getWhereClause(companyId: string, startDate: DateTime, endDate: DateTime, timeZone: String, filters: { [key: string]: string }) {
        const query: { [key: string]: string } = filters;

        // mandatory conditions
        let conditions = `(reportData.submittedAt BETWEEN "${startDate.setZone('utc').toFormat("yyyy-MM-dd HH:mm:ss z")}" AND "${endDate.setZone('utc').toFormat("yyyy-MM-dd HH:mm:ss z")}")`;
        conditions += ` AND (requestData.timestamp BETWEEN "${startDate.setZone('utc').toFormat("yyyy-MM-dd HH:mm:ss z")}" AND "${endDate.setZone('utc').toFormat("yyyy-MM-dd HH:mm:ss z")}")`;

        // optional conditions
        if (companyId) conditions += `AND reportData.companyId = "${companyId}" AND requestData.companyId = "${companyId}"`;
        if (query.status) conditions += ` AND reportData.status in (${getQuotedStrings(query.status.splitAndTrim(','))})`;

        return conditions;
    }

    public async getLogs(companyId: string, startDate: DateTime, endDate: DateTime, timeZone: string = DEFAULT_TIMEZONE, filters: { [key: string]: string } = {}, fields: string[] = []) {
        const query: string = this.getQuery(companyId, startDate, endDate, timeZone, filters, fields);
        const data = await getQueryResults(query);
        return { data };
    }
}

export default WaLogsService.getSingletonInstance();
