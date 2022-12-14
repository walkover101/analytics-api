
import { getQueryResults, MSG91_DATASET_ID, MSG91_PROJECT_ID, OTP_TABLE_ID } from '../../database/big-query-service';
import { convertCodesToMessage, getValidFields, prepareQuery } from '../utility-service';
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
    19: "Price Range Blocked",
    20: "Country Code Blocked",
    25: "Rejected",
    26: "Delivered",
    28: "Invalid Number",
    29: "Invalid Number",
}
const BLOCKED_CREDITS = {
    9: 0,
    17: 0,
    18: 0,
    19: 0,
    20: 0
}
const DEFAULT_TIMEZONE: string = 'Asia/Kolkata';
const PERMITTED_FIELDS: { [key: string]: string } = {
    sentDateTime: `STRING(TIMESTAMP_TRUNC(otpData.sentTime, SECOND))`,
    requestId: "otpData.id",
    telNum: "otpData.telNum",
    status: convertCodesToMessage('otpData.reportStatus', STATUS_CODES),
    senderId: "otpData.requestSender",
    deliveryDate: 'STRING(DATE(otpData.deliveryTime))',
    deliveryTime: 'STRING(TIME(otpData.deliveryTime))',
    credit: convertCodesToMessage('otpData.reportStatus', BLOCKED_CREDITS, false, 'otpData.credit'),
    msgLength: "otpData.credits",
    pauseReason: "otpData.pauseReason",
    requestUserid: "otpData.requestUserid",
    voiceRetryCount: "otpData.voiceRetryCount",
    otpRetry: "otpData.otpRetry",
    verified: "otpData.verified",
    otpVerCount: "otpData.otpVerCount",
    failureReason: "otpData.failureReason",
};
const REPORT_FIELDS: string[] = ['id'].concat(['sentTime', 'deliveryTime', 'credit', 'credits', 'pauseReason',
    'requestUserid', 'voiceRetryCount', 'otpRetry', 'verified', 'otpVerCount',
    'telNum', 'reportStatus', 'requestSender', 'failureReason'].map(field => `ARRAY_AGG(${field} ORDER BY timestamp DESC)[OFFSET(0)] AS ${field}`));


class OtpLogsService {
    private static instance: OtpLogsService;
    public static getSingletonInstance(): OtpLogsService {
        return OtpLogsService.instance ||= new OtpLogsService();
    }

    public getQuery(companyId: string, startDate: DateTime, endDate: DateTime, timeZone: string = DEFAULT_TIMEZONE, filters: { [key: string]: string } = {}, fields: string[] = []) {
        startDate = startDate.setZone(timeZone).set({ hour: 0, minute: 0, second: 0 });
        endDate = endDate.plus({ days: 1 }).setZone(timeZone).set({ hour: 0, minute: 0, second: 0 });
        const attributes = getValidFields(PERMITTED_FIELDS, fields).withAlias.join(',');
        const whereClause = this.getWhereClause(companyId, startDate, endDate, timeZone, filters);
        const query = `SELECT ${attributes} 
            FROM (${prepareQuery(OTP_TABLE_ID, REPORT_FIELDS, whereClause, 'id')}) AS otpData`;
        logger.info(query);
        return query;
    }

    private getWhereClause(companyId: string, startDate: DateTime, endDate: DateTime, timeZone: String, filters: { [key: string]: string }) {
        const query: { [key: string]: string } = filters;

        // mandatory conditions
        let conditions = `(requestDate BETWEEN "${startDate.setZone('utc').toFormat("yyyy-MM-dd HH:mm:ss z")}" AND "${endDate.setZone('utc').toFormat("yyyy-MM-dd HH:mm:ss z")}")`;

        // optional conditions
        if (companyId) conditions += `AND requestUserid = "${companyId}"`;

        return conditions;
    }

    public async getLogs(companyId: string, startDate: DateTime, endDate: DateTime, timeZone: string = DEFAULT_TIMEZONE, filters: { [key: string]: string } = {}, fields: string[] = []) {
        const query: string = this.getQuery(companyId, startDate, endDate, timeZone, filters, fields);
        const data = await getQueryResults(query);
        return { data };
    }
}

export default OtpLogsService.getSingletonInstance();
