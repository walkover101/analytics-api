import { Table } from '@google-cloud/bigquery';
import msg91Dataset, { SMS_FAILED_REPORT_TABLE_ID } from '../database/big-query-service';
import z from 'zod';
import { DateTime } from 'luxon';
const failedReportTable: Table = msg91Dataset.table(SMS_FAILED_REPORT_TABLE_ID);

export const failedReportSchema = z.object({
    _id: z.string(),
    requestID: z.string().optional(),
    telNum: z.string().optional(),
    status: z.string().optional(),
    smsc: z.string().optional(),
    user_pid: z.string().optional(),
    senderID: z.string().optional(),
    route: z.string().optional(),
    failTime: z.date().optional(),
    providerSMSID: z.string().optional(),
    description: z.string().optional(),
    retryCount: z.string().optional(),
    oppri: z.string().optional(),
    crcy: z.string().optional(),
    insertTime: z.date().optional(),
    timestamp: z.string()
});

export type FailedReport = z.infer<typeof failedReportSchema>;

export default class FailedReportData {
    data: FailedReport;
    constructor(attr: any) {
        this.data = {
            _id: (attr['_id']).toString(),
            requestID: attr['requestID'],
            telNum: attr['telNum'],
            status: attr['status'],
            smsc: attr['smsc'],
            user_pid: attr['user_pid'],
            senderID: attr['senderID'],
            route: attr['route'],
            failTime: new Date(attr['failTime']),
            providerSMSID: attr['providerSMSID'],
            description: attr['description'],
            retryCount: attr['retryCount'],
            oppri: attr['oppri'],
            crcy: attr['crcy'],
            insertTime: attr['insertTime'],
            timestamp: attr['timestamp']
        };

    }

    public static insertMany(rows: Array<FailedReport>) {
        const insertOptions = { skipInvalidRows: true, ignoreUnknownValues: true };
        return failedReportTable.insert(rows, insertOptions);
    }
}