import { Table } from '@google-cloud/bigquery';
import msg91Dataset, { SMS_FAILED_REPORT_TABLE_ID } from '../database/big-query-service';
import z from 'zod';
const failedReportTable: Table = msg91Dataset.table(SMS_FAILED_REPORT_TABLE_ID);

export const failedReportSchema = z.object({
    _id: z.string(),
    requestID: z.string(),
    telNum: z.string(),
    status: z.string(),
    smsc: z.string(),
    user_pid: z.string(),
    senderID: z.string(),
    route: z.string(),
    failTime: z.date(),
    providerSMSID: z.string(),
    description: z.string(),
    retryCount: z.string(),
    oppri: z.string(),
    crcy: z.string(),
    insertTime: z.date(),
    timestamp: z.string()
});

export type FailedReport = z.infer<typeof failedReportSchema>;

export default class FailedReportData {
    data: FailedReport;
    constructor(attr: any) {
        this.data = {
            _id: attr['_id'],
            requestID: attr['requestID'],
            telNum: attr['telNum'],
            status: attr['status'],
            smsc: attr['smsc'],
            user_pid: attr['user_pid'],
            senderID: attr['senderID'],
            route: attr['route'],
            failTime: attr['failTime'],
            providerSMSID: attr['providerSMSID'],
            description: attr['description'],
            retryCount: attr['retryCount'],
            oppri: attr['oppri'],
            crcy: attr['crcy'],
            insertTime: attr['insertTime'],
            timestamp: attr['timestamp']
        };

    }

    public static insertMany(rows: Array<FailedReportData>) {
        const insertOptions = { skipInvalidRows: true, ignoreUnknownValues: true };
        const data = rows.map(row => row.data);
        return failedReportTable.insert(data, insertOptions);
    }
}