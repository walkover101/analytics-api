import { Table } from '@google-cloud/bigquery';
import msg91Dataset, { WA_REQ_TABLE_ID } from '../database/big-query-service';

const waRequestTable: Table = msg91Dataset.table(WA_REQ_TABLE_ID);

export default class WARequest {
    uuid: string;
    companyId: string;
    integratedNumber: string;
    customerNumber: string;
    vendorId: string;
    messageType: string;
    direction: number;
    timestamp: Date;
    content: string;
    status: string;
    nodeId: string;

    constructor(attr: any) {
        this.uuid = attr['message_uuid']; // Uniquely identifies each request
        this.companyId = attr['company_id']; // Comapany Id 
        this.integratedNumber = attr['integrated_number']; // Uniquely assigned phone number to company by WA
        this.customerNumber = attr['customer_number']; // WA number of customer
        this.vendorId = attr['vendor_id']; // Vendor that is used to process the message
        this.messageType = attr['message_type']; // Type of message i.e text, attachment etc
        this.direction = attr['direction']; // Inbound / Outbound
        this.timestamp = attr['submitted_at']; // When message was requested
        this.content = attr['content']; // Content of the message
        this.status = attr['status']; // Submitted / Failed
        this.nodeId = attr['node_id']; // Identifies if this message is a part of a campaign
    }

    public static insertMany(rows: Array<WARequest>) {
        const insertOptions = { skipInvalidRows: true, ignoreUnknownValues: true };
        return waRequestTable.insert(rows, insertOptions);
    }
}