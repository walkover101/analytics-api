
import { intersection } from 'lodash';
import { DateTime } from 'luxon';

export enum DOWNLOAD_STATUS {
    PENDING = 'PENDING',
    PROCESSING = 'PROCESSING',
    SUCCESS = 'SUCCESS',
    ERROR = 'ERROR'
}

const DEFAULT_FIELDS: { [key: string]: string } = {
    // from report-data
    status: 'reportData.status',
    sentTime: 'reportData.sentTime',
    deliveryTime: 'reportData.deliveryTime',
    requestId: 'reportData.requestID',
    route: 'reportData.route',
    telNum: 'reportData.telNum',
    credit: 'reportData.credit',
    senderId: 'reportData.senderID',

    // from request-data
    campaignName: 'requestData.campaign_name',
    scheduleDateTime: 'requestData.scheduleDateTime',
    msgData: 'requestData.msgData'
}

export default class Download {
    id?: string;
    companyId: string;
    startDate: DateTime;
    endDate: DateTime;
    status: DOWNLOAD_STATUS = DOWNLOAD_STATUS.PENDING;
    fields: Array<string>;
    files?: Array<string>;
    route?: Array<string>;
    err?: string;
    createdAt: Date = new Date();
    updatedAt: Date = new Date();

    constructor(companyId: string, startDate: DateTime, endDate: DateTime, fields: string = '', route?: string) {
        this.companyId = companyId;
        this.startDate = startDate;
        this.endDate = endDate;
        if (route) this.route = route.split(',');
        this.fields = this.getValidFields(fields.split(','));
    }

    getValidFields(fields: Array<string>) {
        if (!fields.length) return [];
        const result: string[] = [];
        let attrbs = intersection(Object.keys(DEFAULT_FIELDS), fields);
        if (!attrbs.length) attrbs = Object.keys(DEFAULT_FIELDS);
        attrbs.map(key => result.push(DEFAULT_FIELDS[key]));
        return result;
    }
}