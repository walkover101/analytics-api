
import { getQueryResults, REPORT_DATA_TABLE_ID, REQUEST_DATA_TABLE_ID } from '../../database/big-query-service';
import { convertCodesToMessage, getQuotedStrings, getValidFields } from '../utility-service';
import { DateTime } from 'luxon';
import logger from '../../logger/logger';

const STATUS_CODES = {
    1: "Delivered",
    2: "Failed",
    3: "Delivered",
    5: "Pending",
    6: "Submitted",
    7: "Auto Failed",
    8: "Sent",
    9: "NDNC Number",
    13: "Failed",
    16: "Rejected By Provider",
    17: "Blocked Number",
    18: "Blocked Circle",
    20: "Country Code Blocked",
    25: "Rejected",
    26: "Delivered",
    28: "Invalid Number",
    29: "Invalid Number",
}
const DEFAULT_TIMEZONE: string = 'Asia/Kolkata';
const PERMITTED_FIELDS: { [key: string]: string } = {
    // from report-data
    sentTime: `STRING(TIMESTAMP_TRUNC(DATETIME(reportData.sentTime,'${DEFAULT_TIMEZONE}'), SECOND))`,
    status: convertCodesToMessage('reportData.status', STATUS_CODES),
    deliveryDate: 'STRING(DATE(reportData.deliveryTime))',
    deliveryTime: 'STRING(TIME(reportData.deliveryTime))',
    requestId: 'reportData.requestID',
    telNum: 'reportData.telNum',
    credit: 'reportData.credit',
    senderId: 'reportData.senderID',

    // from request-data
    campaignName: 'requestData.campaign_name',
    scheduleDateTime: 'STRING(requestData.scheduleDateTime)',
    msgData: 'requestData.msgData',
    route: 'requestData.curRoute'
};

class SmsLogsService {
    private static instance: SmsLogsService;


    public static getSingletonInstance(): SmsLogsService {
        return SmsLogsService.instance ||= new SmsLogsService();
    }

    public getQuery(companyId: string, startDate: DateTime, endDate: DateTime, timeZone: string = DEFAULT_TIMEZONE, filters: { [key: string]: string } = {}, fields: string[] = []) {
        startDate = startDate.setZone(timeZone).set({ hour: 0, minute: 0, second: 0 });
        endDate = endDate.plus({ days: 1 }).setZone(timeZone).set({ hour: 0, minute: 0, second: 0 });
        const attributes = getValidFields(PERMITTED_FIELDS, fields).withAlias.join(',');
        const whereClause = this.getWhereClause(companyId, startDate, endDate, timeZone, filters);
        const query = `SELECT ${attributes} 
            FROM ${REPORT_DATA_TABLE_ID} as reportData 
            JOIN ${REQUEST_DATA_TABLE_ID} as requestData 
            ON reportData.requestId = requestData._id 
            WHERE ${whereClause}`;
        logger.info(query);
        return query;
    }

    private getWhereClause(companyId: string, startDate: DateTime, endDate: DateTime, timeZone: String, filters: { [key: string]: string }) {
        const query: { [key: string]: string } = filters;

        // mandatory conditions
        let conditions = `(reportData.sentTime BETWEEN "${startDate.setZone('utc').toFormat("yyyy-MM-dd HH:mm:ss z")}" AND "${endDate.plus({ days: 1 }).setZone('utc').toFormat("yyyy-MM-dd HH:mm:ss z")}")`;
        conditions += ` AND (requestData.requestDate BETWEEN "${startDate.setZone('utc').toFormat("yyyy-MM-dd HH:mm:ss z")}" AND "${endDate.setZone('utc').toFormat("yyyy-MM-dd HH:mm:ss z")}")`;

        // optional conditions
        if (companyId) conditions += `AND reportData.user_pid = "${companyId}" AND requestData.requestUserid = "${companyId}"`;
        if (query.route) conditions += ` AND requestData.curRoute in (${getQuotedStrings(query.route.splitAndTrim(','))})`;

        return conditions;
    }

    public async getLogs(companyId: string, startDate: DateTime, endDate: DateTime, timeZone: string = DEFAULT_TIMEZONE, filters: { [key: string]: string } = {}, fields: string[] = []) {
        const query: string = this.getQuery(companyId, startDate, endDate, timeZone, filters, fields);
        const data = await getQueryResults(query);
        return { data };
    }
}

export default SmsLogsService.getSingletonInstance();
