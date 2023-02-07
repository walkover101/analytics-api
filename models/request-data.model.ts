import { Table } from '@google-cloud/bigquery';
import msg91Dataset, { REQUEST_DATA_TABLE_ID } from '../database/big-query-service';
import z from 'zod';
const requestDataTable: Table = msg91Dataset.table(REQUEST_DATA_TABLE_ID);

const request = z.object({
    _id: z.string(),
    requestID: z.string(),
    telNum: z.string(),
    reportStatus: z.number(),
    sentTimeReport: z.date(),
    providerSMSID: z.string(),
    user_pid: z.string(),
    senderID: z.string(),
    smsc: z.string(),
    requestRoute: z.string(),
    campaign_name: z.string(),
    campaign_pid: z.string(),
    curRoute: z.string(),
    expiry: z.string(),
    isCopied: z.string(),
    requestDate: z.date(),
    userCountryCode: z.string(),
    requestUserid: z.string(),
    status: z.string(),
    userCredit: z.string(),
    isSingleRequest: z.string(),
    deliveryTime: z.date(),
    route: z.string(),
    credit: z.number(),
    credits: z.number(),
    oppri: z.number(),
    crcy: z.string(),
    node_id: z.string(),
    scheduleDateTime: z.date(),
    msgData: z.string(),
    plugin: z.string().optional(),
    timestamp: z.string()
});
type Request = z.infer<typeof request>;
export default class RequestData {

    data: Request;

    constructor(attr: any) {
        this.data = {
            _id: attr['_id'].toString(),
            requestID: attr['requestID'],
            telNum: attr['telNum'],
            reportStatus: attr['reportStatus'],
            sentTimeReport: attr['sentTimeReport'] || null,
            providerSMSID: attr['providerSMSID'],
            user_pid: attr['user_pid'],
            senderID: attr['senderID'],
            smsc: attr['smsc'],
            requestRoute: attr['requestRoute'],
            campaign_name: attr['campaign_name'],
            campaign_pid: attr['campaign_pid'],
            curRoute: attr['curRoute'],
            expiry: attr['expiry'],
            isCopied: attr['isCopied'],
            requestDate: attr['requestDate'] || null,
            userCountryCode: attr['userCountryCode'],
            requestUserid: attr['requestUserid'],
            status: attr['status'],
            userCredit: attr['userCredit'],
            isSingleRequest: attr['isSingleRequest'],
            deliveryTime: attr['deliveryTime'] || null,
            route: attr['route'],
            credit: parseFloat(attr['credit']),
            credits: +attr['credits'],
            oppri: parseFloat(attr['oppri'] || 0),
            crcy: attr['crcy'],
            node_id: attr['node_id'],
            scheduleDateTime: attr['scheduleDateTime'] || null,
            msgData: attr['msgData'],
            plugin: attr['plugin'] || null,
            timestamp: attr['timestamp']
        }
    }

    public static insertMany(rows: Array<RequestData>) {
        const insertOptions = { skipInvalidRows: true, ignoreUnknownValues: true };
        return requestDataTable.insert(rows.map(row => row.data), insertOptions);
    }
}