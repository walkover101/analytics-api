import { Table } from '@google-cloud/bigquery';
import msg91Dataset from './big-query-service';
import DlrLog from '../models/dlr-log.model';

const DLR_LOGS_TABLE_ID = process.env.DLR_LOGS_TABLE_ID || 'dlr_logs'

class DlrLogsService {
    private static instance: DlrLogsService;
    private dlrLogsTable: Table;

    constructor() {
        this.dlrLogsTable = msg91Dataset.table(DLR_LOGS_TABLE_ID);
    }

    public static getSingletonInstance(): DlrLogsService {
        return DlrLogsService.instance ||= new DlrLogsService();
    }

    public insertMany(rows: Array<DlrLog>) {
        const insertOptions = { skipInvalidRows: true, ignoreUnknownValues: true };
        return this.dlrLogsTable.insert(rows, insertOptions);
    }
}

export default DlrLogsService.getSingletonInstance();
