import { Table } from '@google-cloud/bigquery';
import logger from "../logger/logger";
import msg91Dataset, { prepareDocument } from './big-query-service';

const REPORT_DATA_TABLE_ID = process.env.REPORT_DATA_TABLE_ID || 'report_data'
const reportDataSchema = ['_id', 'requestID', 'telNum', 'status', 'sentTime', 'providerSMSID', 'user_pid', 'senderID', 'smsc', 'deliveryTime', 'route', 'credit', 'retryCount', 'sentTimePeriod', 'crcy', 'node_id'];

class ReportDataService {
    private static instance: ReportDataService;
    private reportDataTable: Table;

    constructor() {
        this.reportDataTable = msg91Dataset.table(REPORT_DATA_TABLE_ID);
    }

    public static getSingletonInstance(): ReportDataService {
        return ReportDataService.instance ||= new ReportDataService();
    }

    public insertMany(rows: Array<Object>) {
        const insertOptions = { skipInvalidRows: true, ignoreUnknownValues: true };
        return this.reportDataTable.insert(rows, insertOptions);
    }

    public prepareDocument(doc: any) {
        return prepareDocument(reportDataSchema, doc);
    }
}

export default ReportDataService.getSingletonInstance();
