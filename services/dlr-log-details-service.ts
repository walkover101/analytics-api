import { Table } from '@google-cloud/bigquery';
import msg91Dataset from '../database/big-query-service';
import DlrLogDetail from '../models/dlr-log-detail.model';

const DLR_LOG_DETAILS_TABLE_ID = process.env.DLR_LOG_DETAILS_TABLE_ID || 'dlr_log_details'

class DlrLogDetailsService {
    private static instance: DlrLogDetailsService;
    private dlrLogDetailsTable: Table;

    constructor() {
        this.dlrLogDetailsTable = msg91Dataset.table(DLR_LOG_DETAILS_TABLE_ID);
    }

    public static getSingletonInstance(): DlrLogDetailsService {
        return DlrLogDetailsService.instance ||= new DlrLogDetailsService();
    }

    public insertMany(rows: Array<DlrLogDetail>) {
        const insertOptions = { skipInvalidRows: true, ignoreUnknownValues: true };
        return this.dlrLogDetailsTable.insert(rows, insertOptions);
    }
}

export default DlrLogDetailsService.getSingletonInstance();
