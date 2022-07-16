
import { time } from 'console';
import { intersection } from 'lodash';
import { DateTime } from 'luxon';

export enum EXPORT_STATUS {
    PENDING = 'PENDING',
    PROCESSING = 'PROCESSING',
    SUCCESS = 'SUCCESS',
    ERROR = 'ERROR'
}

const DEFAULT_FIELDS = [
    // from report-data
    'status',
    'sentTime',
    'deliveryTime',
    'requestID',
    'route',
    'telNum',
    'credit',
    'senderID',

    // from request-data
    'campaign_name',
    'scheduleDate',
    'scheduleTime',
    'msgData'
];

export default class ExportReport {
    id?: string;
    companyId: string;
    startDate: DateTime;
    endDate: DateTime;
    status: EXPORT_STATUS = EXPORT_STATUS.PENDING;
    fields: Array<string> = DEFAULT_FIELDS;
    files?: Array<string>;
    route?: string;
    err?: string;

    constructor(companyId: string, startDate: DateTime, endDate: DateTime, fields: string = '', route?: string) {
        this.companyId = companyId;
        this.startDate = startDate;
        this.endDate = endDate;
        this.route = route;
        this.fields = this.getValidFields(fields.split(','));
    }

    getValidFields(fields: Array<string>) {
        if (!fields.length) return [];
        let result = intersection(DEFAULT_FIELDS, fields);
        if (!result.length) result = DEFAULT_FIELDS;
        return result;
    }
}