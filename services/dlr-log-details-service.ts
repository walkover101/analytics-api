import { Table } from '@google-cloud/bigquery';
import msg91Dataset from './big-query-service';
import DlrLogDetail from '../models/dlr-log-detail.model';

const DLR_LOGS_TABLE_ID = process.env.DLR_LOGS_TABLE_ID || 'dlr-logs'

class DlrLogDetailsService {
    private static instance: DlrLogDetailsService;
    private dlrLogDetailsTable: Table;

    constructor() {
        this.dlrLogDetailsTable = msg91Dataset.table(DLR_LOGS_TABLE_ID);
    }

    public static getSingletonInstance(): DlrLogDetailsService {
        return DlrLogDetailsService.instance ||= new DlrLogDetailsService();
    }

    public insertMany(rows: Array<DlrLogDetail>) {
        const insertOptions = { skipInvalidRows: true, ignoreUnknownValues: true };
        rows.map(dlrlogDetail => console.log(dlrlogDetail));
        // return this.dlrLogDetailsTable.insert(rows, insertOptions);
    }
}

export default DlrLogDetailsService.getSingletonInstance();
