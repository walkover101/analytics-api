import { Table } from '@google-cloud/bigquery';
import * as _ from "lodash";

import msg91Dataset from './big-query-service';
import RequestData from '../models/request-data.model';

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

    public insertMany(rows: Array<RequestData>) {
        const insertOptions = { skipInvalidRows: true, ignoreUnknownValues: true };
        return this.requestDataTable.insert(rows, insertOptions);
    }
}

export default RequestDataService.getSingletonInstance();
