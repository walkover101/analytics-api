import { Table } from '@google-cloud/bigquery';
import msg91Dataset from './big-query-service';
import ReportData from '../models/report-data.model';
import ExportReport from '../models/export-report.model';

const REPORT_DATA_TABLE_ID = process.env.REPORT_DATA_TABLE_ID || 'report_data'
const GCS_BUCKET_NAME = 'msg91-analytics';
const GCS_FOLDER_NAME = 'report-data-exports';

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

    public export(fileName: string, exportReport: ExportReport, format: string = 'CSV') {
        const exportFilePath = `gs://${GCS_BUCKET_NAME}/${GCS_FOLDER_NAME}/${fileName}_*.csv`;
        const overwrite = true;
        const header = true;
        const fieldDelimiter = ';';
        const queryStatement = `select * from ${REPORT_DATA_TABLE_ID} WHERE user_pid = "${exportReport.companyId}" AND (DATE(sentTime) BETWEEN "${exportReport.startDate.toFormat('yyyy-MM-dd')}" AND "${exportReport.endDate.toFormat('yyyy-MM-dd')}") ${exportReport.route ? `AND route = "${exportReport.route}"` : ''} limit 1000`;
        const query = `EXPORT DATA OPTIONS(uri='${exportFilePath}', format='${format}', overwrite=${overwrite}, header=${header}, field_delimiter='${fieldDelimiter}') AS ${queryStatement}`;

        return msg91Dataset.createQueryJob({ query });
    }
}

export default ReportDataService.getSingletonInstance();
