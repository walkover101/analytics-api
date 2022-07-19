
import { intersection } from 'lodash';
import { DateTime } from 'luxon';
import { getQuotedStrings } from '../services/utility-service';

export enum DOWNLOAD_STATUS {
    PENDING = 'PENDING',
    PROCESSING = 'PROCESSING',
    SUCCESS = 'SUCCESS',
    ERROR = 'ERROR'
}

export enum RESOURCE_TYPE {
    SMS = 'sms',
    EMAIL = 'email'
}

export default class Download {
    id?: string;
    resourceType: RESOURCE_TYPE;
    companyId: string;
    startDate: DateTime;
    endDate: DateTime;
    status: DOWNLOAD_STATUS = DOWNLOAD_STATUS.PENDING;
    fields: Array<string>;
    files?: Array<string>;
    query?: { [key: string]: string };
    err?: string;
    createdAt: Date = new Date();
    updatedAt: Date = new Date();

    constructor(resourceType: string, companyId: string, startDate: DateTime, endDate: DateTime, fields: string = '', query: any) {
        this.resourceType = resourceType === RESOURCE_TYPE.EMAIL ? RESOURCE_TYPE.EMAIL : RESOURCE_TYPE.SMS;
        this.companyId = companyId;
        this.startDate = startDate;
        this.endDate = endDate;
        this.query = query;
        this.fields = fields.split(',');
    }
}