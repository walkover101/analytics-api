
import { getQueryResults, MSG91_DATASET_ID, MSG91_PROJECT_ID, OTP_TABLE_ID } from '../../database/big-query-service';
import { getValidFields } from '../utility-service';
import { DateTime } from 'luxon';
import logger from '../../logger/logger';

const DEFAULT_TIMEZONE: string = 'Asia/Kolkata';
const PERMITTED_FIELDS: { [key: string]: string } = {
    status: `CASE otpData.reportStatus 
    WHEN 1 THEN "Delivered" 
    WHEN 26 THEN "Delivered" 
    WHEN 3 THEN "Delivered" 
    WHEN 2 THEN "Failed" 
    WHEN 13 THEN "Failed" 
    WHEN 7 THEN "Auto Failed" 
    WHEN 9 THEN "NDNC Number" 
    WHEN 25 THEN "Rejected" 
    WHEN 16 THEN "Rejected By Provider" 
    WHEN 17 THEN "Blocked Number" 
    WHEN 18 THEN "Blocked Circle" 
    WHEN 20 THEN "Country Code Blocked" 
    WHEN 28 THEN "Invalid Number" 
    WHEN 29 THEN "Invalid Number" 
    WHEN 6 THEN "Submitted" 
    WHEN 5 THEN "Pending" 
    WHEN 8 THEN "Sent" 
    ELSE CAST(otpData.reportStatus AS STRING) END`,
    telNum: "otpData.telNum",
    oppri: "otpData.oppri",
    crcy: "otpData.crcy",
    sentTimeReport: `STRING(DATETIME(otpData.sentTimeReport,'${DEFAULT_TIMEZONE}'))`,
    providerSmsid: "otpData.providerSmsid",
    smsc: "otpData.smsc",
    description: "otpData.description",
    requestRoute: "otpData.requestRoute",
    campaignName: "otpData.campaignName",
    campaignPid: "otpData.campaignPid",
    credits: "otpData.credits",
    expiry: "otpData.expiry",
    requestDate: "STRING(DATE(otpData.requestDate))",
    countryCode: "otpData.countryCode",
    pauseReason: "otpData.pauseReason",
    requestDateString: "STRING(DATE(otpData.requestDateString))",
    noOfSms: "otpData.noOfSms",
    requestUserid: "otpData.requestUserid",
    requestSender: "otpData.requestSender",
    sentTime: `STRING(DATETIME(otpData.sentTime,'${DEFAULT_TIMEZONE}'))`,
    unicode: "otpData.unicode",
    userCredit: "otpData.userCredit",
    templateId: "otpData.templateId",
    extraParam: "otpData.extraParam",
    dcc: "otpData.dcc",
    peId: "otpData.peId",
    dltTeId: "otpData.dltTeId",
    otpRetry: "otpData.otpRetry",
    verified: "otpData.verified",
    otpVerCount: "otpData.otpVerCount",
    deliveryDate: 'STRING(DATE(otpData.deliveryTime))',
    deliveryTime: 'STRING(TIME(otpData.deliveryTime))',
    dts: "otpData.dts",
    route: "otpData.route",
    credit: "otpData.credit",
    retryCount: "otpData.retryCount",
    voiceReportTime: "otpData.voiceReportTime",
    voiceResponse: "otpData.voiceResponse",
    voiceRetryCount: "otpData.voiceRetryCount",
    voiceService: "otpData.voiceService",
    voiceStatus: "otpData.voiceStatus",
    demoAccount: "otpData.demoAccount",
    source: "otpData.source"
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
