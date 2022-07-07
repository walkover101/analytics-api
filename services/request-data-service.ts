import { Table } from '@google-cloud/bigquery';
import msg91Dataset from '../startup/big-query';

const REQUEST_DATA_TABLE_ID = process.env.REQUEST_DATA_TABLE_ID || 'new_request_data'

class RequestDataService {
    private static instance: RequestDataService;
    private requestDataTable: Table;

    constructor() {
        this.requestDataTable = msg91Dataset.table(REQUEST_DATA_TABLE_ID);
    }

    public static getSingletonInstance(): RequestDataService {
        RequestDataService.instance ||= new RequestDataService();
        return RequestDataService.instance;
    }

    public insertMany(rows: Array<Object>) {
        try {
            const insertOptions = { skipInvalidRows: true, ignoreUnknownValues: true };
            return this.requestDataTable.insert(rows, insertOptions);
        } catch (err: any) {
            if (err.name !== 'PartialFailureError') throw err;
            console.log("[RequestDataService:insertMany](PartialFailureError)", err);
        }
    }
}

export default RequestDataService.getSingletonInstance();
