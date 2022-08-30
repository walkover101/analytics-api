
import { getQueryResults, MSG91_DATASET_ID, MSG91_PROJECT_ID, OTP_TABLE_ID } from '../../database/big-query-service';
import { getValidFields } from '../utility-service';
import { DateTime } from 'luxon';
import logger from '../../logger/logger';

const DEFAULT_TIMEZONE: string = 'Asia/Kolkata';
const PERMITTED_FIELDS: { [key: string]: string } = {
    telNum: "otpData.telNum",
    oppri: "otpData.oppri",
    crcy: "otpData.crcy",
    reportStatus: "otpData.reportStatus",
    sentTimeReport: "STRING(DATE(otpData.sentTimeReport))",
    providerSmsid: "otpData.providerSmsid",
    smsc: "otpData.smsc",
    description: "otpData.description",
    requestRoute: "otpData.requestRoute",
    campaignName: "otpData.campaignName",
    campaignPid: "otpData.campaignPid",
    credits: "otpData.credits",
    expiry: "otpData.expiry",
    mobiles: "otpData.mobiles",
    requestDate: "STRING(DATE(otpData.requestDate))",
    msgData: "otpData.msgData",
    countryCode: "otpData.countryCode",
    pauseReason: "otpData.pauseReason",
    requestDateString: "STRING(DATE(otpData.requestDateString))",
    noOfSms: "otpData.noOfSms",
    requestUserid: "otpData.requestUserid",
    otp: "otpData.otp",
    requestSender: "otpData.requestSender",
    sentTime: "STRING(DATE(otpData.sentTime))",
    unicode: "otpData.unicode",
    status: "otpData.status",
    userCredit: "otpData.userCredit",
    templateId: "otpData.templateId",
    extraParam: "otpData.extraParam",
    dcc: "otpData.dcc",
    peId: "otpData.peId",
    dltTeId: "otpData.dltTeId",
    otpRetry: "otpData.otpRetry",
    verified: "otpData.verified",
    otpVerCount: "otpData.otpVerCount",
    deliveryTime: "STRING(DATE(otpData.deliveryTime))",
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
        endDate = endDate.plus({days: 1}).setZone(timeZone).set({ hour: 0, minute: 0, second: 0 });
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
