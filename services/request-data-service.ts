import { Table } from '@google-cloud/bigquery';
import * as _ from "lodash";
import logger from "../logger/logger";
import msg91Dataset, { prepareDocument } from './big-query-service';

const REQUEST_DATA_TABLE_ID = process.env.REQUEST_DATA_TABLE_ID || 'request_data'
const requestDataSchema = ['_id', 'requestID', 'telNum', 'reportStatus', 'sentTimeReport', 'providerSMSID', 'user_pid', 'senderID', 'smsc', 'requestRoute', 'campaign_name', 'campaign_pid', 'curRoute', 'expiry', 'isCopied', 'requestDate', 'userCountryCode', 'requestUserid', 'status', 'userCredit', 'isSingleRequest', 'deliveryTime', 'route', 'credit', 'oppri', 'crcy', 'node_id'];

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

    public prepareDocument(doc: any) {
        return prepareDocument(requestDataSchema, doc);
    }
}

export default RequestDataService.getSingletonInstance();
