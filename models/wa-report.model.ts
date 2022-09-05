import { Table } from '@google-cloud/bigquery';
import msg91Dataset, { WA_REP_TABLE_ID } from '../database/big-query-service';

const waReportTable: Table = msg91Dataset.table(WA_REP_TABLE_ID);

export default class WAReport {
    uuid: string;
    companyId: string;
    price: number;
    origin: string;
    windowExp: Date;
    status: string;
    timestamp: Date;
    submittedAt: Date;

    constructor(attr: any) {
        this.uuid = attr['message_uuid']; // Uniquely identifies each request
        this.companyId = attr['company_id']; // Comapany Id
        this.status = attr['status']; // Sent/ Delivered/ Read
        this.timestamp = attr['sent_at'] || attr['delivered_at'] || attr['read_at'];
        this.submittedAt = attr['submitted_at'];
        this.price = attr['price'];
        this.origin = attr['origin'];
        this.windowExp = attr['window_exp'];
    }

    public static insertMany(rows: Array<WAReport>) {
        const insertOptions = { skipInvalidRows: true, ignoreUnknownValues: true };
        return waReportTable.insert(rows, insertOptions);
    }
}