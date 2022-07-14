
import { DateTime } from 'luxon';

export enum EXPORT_STATUS {
    PENDING = 'PENDING',
    PROCESSING = 'PROCESSING',
    SUCCESS = 'SUCCESS',
    ERROR = 'ERROR'
}

export default class ExportReport {
    id?: string;
    companyId: string;
    startDate: DateTime;
    endDate: DateTime;
    status: EXPORT_STATUS = EXPORT_STATUS.PENDING;
    files?: Array<string>;
    route?: string;
    err?: string;

    constructor(companyId: string, startDate: DateTime, endDate: DateTime, route?: string) {
        this.companyId = companyId;
        this.startDate = startDate;
        this.endDate = endDate;
        this.route = route;
    }
}