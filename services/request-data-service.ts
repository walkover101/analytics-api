import { Table } from '@google-cloud/bigquery';
import logger from "../logger/logger";
import msg91Dataset from './big-query-service';

const REQUEST_DATA_TABLE_ID = process.env.REQUEST_DATA_TABLE_ID || 'request_data'

class RequestDataService {
    private static instance: RequestDataService;
    private requestDataTable: Table;

    constructor() {
        this.requestDataTable = msg91Dataset.table(REQUEST_DATA_TABLE_ID);
    }

    public static getSingletonInstance(): RequestDataService {
        return RequestDataService.instance ||= new RequestDataService();
    }

    public insertMany(rows: Array<Object>) {
        try {
            const insertOptions = { skipInvalidRows: true, ignoreUnknownValues: true };
            return this.requestDataTable.insert(rows, insertOptions);
        } catch (err: any) {
            if (err.name !== 'PartialFailureError') throw err;
            logger.log("[RequestDataService:insertMany](PartialFailureError)", err);
        }
    }
}

export default RequestDataService.getSingletonInstance();
