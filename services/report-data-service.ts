import { Table } from '@google-cloud/bigquery';
import msg91Dataset from './big-query-service';
import ReportData from '../models/report-data.model';

const REPORT_DATA_TABLE_ID = process.env.REPORT_DATA_TABLE_ID || 'report_data'

class ReportDataService {
    private static instance: ReportDataService;
    private reportDataTable: Table;

    constructor() {
        this.reportDataTable = msg91Dataset.table(REPORT_DATA_TABLE_ID);
    }

    public static getSingletonInstance(): ReportDataService {
        return ReportDataService.instance ||= new ReportDataService();
    }

    public insertMany(rows: Array<ReportData>) {
        const insertOptions = { skipInvalidRows: true, ignoreUnknownValues: true };
        return this.reportDataTable.insert(rows, insertOptions);
    }
}

export default ReportDataService.getSingletonInstance();
