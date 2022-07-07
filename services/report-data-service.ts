import { Table } from '@google-cloud/bigquery';
import msg91Dataset from '../startup/big-query';

const REPORT_DATA_TABLE_ID = process.env.REPORT_DATA_TABLE_ID || 'report_data'

class ReportDataService {
    private static instance: ReportDataService;
    private reportDataTable: Table;

    constructor() {
        this.reportDataTable = msg91Dataset.table(REPORT_DATA_TABLE_ID);
    }

    public static getSingletonInstance(): ReportDataService {
        ReportDataService.instance ||= new ReportDataService();
        return ReportDataService.instance;
    }

    public insertMany(rows: Array<Object>) {
        try {
            const insertOptions = { skipInvalidRows: true, ignoreUnknownValues: true };
            return this.reportDataTable.insert(rows, insertOptions);
        } catch (err: any) {
            if (err.name !== 'PartialFailureError') throw err;
            console.log("[ReportDataService:insertMany](PartialFailureError)", err);
        }
    }
}

export default ReportDataService.getSingletonInstance();
