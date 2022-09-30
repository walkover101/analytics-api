
import { getQueryResults, MSG91_DATASET_ID, MSG91_PROJECT_ID, OTP_TABLE_ID } from '../../database/big-query-service';
import { convertCodesToMessage, getValidFields } from '../utility-service';
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
    requestDate: `STRING(TIMESTAMP_TRUNC(DATETIME(otpData.requestDate), SECOND))`,
    requestId: "otpData.id",
    telNum: "otpData.telNum",
    sentTime: `STRING(TIMESTAMP_TRUNC(DATETIME(otpData.sentTime,'${DEFAULT_TIMEZONE}'), SECOND))`,
    status: convertCodesToMessage('otpData.reportStatus', STATUS_CODES),
    senderId: "otpData.requestSender",
    deliveryDate: 'STRING(DATE(otpData.deliveryTime))',
    deliveryTime: 'STRING(TIME(otpData.deliveryTime))',
    credit: "otpData.credit",
    userCredit: "otpData.userCredit",
    description: "otpData.description",
    pauseReason: "otpData.pauseReason",
    requestUserid: "otpData.requestUserid",
    voiceRetryCount: "otpData.voiceRetryCount",
    otpRetry: "otpData.otpRetry",
    verified: "otpData.verified",
    otpVerCount: "otpData.otpVerCount",
};

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
            FROM \`${MSG91_PROJECT_ID}.${MSG91_DATASET_ID}.${OTP_TABLE_ID}\` AS otpData 
            WHERE ${whereClause}`;
        logger.info(query);
        return query;
    }

    private getWhereClause(companyId: string, startDate: DateTime, endDate: DateTime, timeZone: String, filters: { [key: string]: string }) {
        const query: { [key: string]: string } = filters;

        // mandatory conditions
        let conditions = `(otpData.requestDate BETWEEN "${startDate.setZone('utc').toFormat("yyyy-MM-dd HH:mm:ss z")}" AND "${endDate.setZone('utc').toFormat("yyyy-MM-dd HH:mm:ss z")}")`;

        // optional conditions
        if (companyId) conditions += `AND otpData.requestUserid = "${companyId}"`;

        return conditions;
    }

    public async getLogs(companyId: string, startDate: DateTime, endDate: DateTime, timeZone: string = DEFAULT_TIMEZONE, filters: { [key: string]: string } = {}, fields: string[] = []) {
        const query: string = this.getQuery(companyId, startDate, endDate, timeZone, filters, fields);
        const data = await getQueryResults(query);
        return { data };
    }
}

export default OtpLogsService.getSingletonInstance();
