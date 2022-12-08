import { Table } from '@google-cloud/bigquery';
import msg91Dataset, { REPORT_DATA_TABLE_ID } from '../database/big-query-service';
import { extractCountryCode, getFailureReason } from "../services/utility-service";
import z from 'zod';
const reportDataTable: Table = msg91Dataset.table(REPORT_DATA_TABLE_ID);

const report = z.object({
    _id: z.string(),
    requestID: z.string(),
    telNum: z.string(),
    countryCode: z.string(),
    status: z.number(),
    sentTime: z.date(),
    providerSMSID: z.string(),
    user_pid: z.string(),
    senderID: z.string(),
    smsc: z.string(),
    description: z.string(),
    failureReason: z.string().optional(),
    deliveryTime: z.date(),
    route: z.string(),
    credit: z.number(),
    retryCount: z.number(),
    sentTimePeriod: z.date(),
    crcy: z.string(),
    node_id: z.string(),
    oppri: z.number(),
    isSingleRequest: z.string(),
    message: z.string(),

})

export const reportKeys = report.keyof();
type Report = z.infer<typeof report>;

export default class ReportData {
    data: Report;
    private constructor(attr: any) {
        this.data = {
            _id: attr['_id']?.toString(),
            requestID: attr['requestID'],
            telNum: attr['telNum'],
            countryCode: extractCountryCode(attr['telNum'])?.regionCode,
            status: parseInt(attr['status']),
            sentTime: attr['sentTime'] || null,
            providerSMSID: attr['providerSMSID'],
            user_pid: attr['user_pid'],
            senderID: attr['senderID'],
            smsc: attr['smsc'],
            description: attr['description'],
            deliveryTime: attr['deliveryTime'] || null,
            route: attr['route'],
            credit: parseFloat(attr['credit']),
            retryCount: parseInt(attr['retryCount']),
            sentTimePeriod: attr['sentTimePeriod'] || null,
            crcy: attr['crcy'],
            node_id: attr['node_id'],
            oppri: parseFloat(attr['oppri'] || 0),
            isSingleRequest: attr['isSingleRequest'],
            message: attr['message']
        }
    }

    public static createAsync = async (attr: any) => {
        const reportData: ReportData = new ReportData(attr);
        reportData.data.failureReason = await getFailureReason(reportData.data.smsc, reportData.data.description);
        return reportData;
    }

    public static insertMany(rows: Array<ReportData>) {
        const insertOptions = { skipInvalidRows: true, ignoreUnknownValues: true };
        return reportDataTable.insert(rows.map(row => row.data), insertOptions);
    }
}