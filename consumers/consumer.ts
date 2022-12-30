import { mailEvent } from './mail-events-consumer';
import { mailRequests } from './mail-requests-consumer';
import { mailReports } from './mail-reports-consumer';
import { waReport } from './wa-reports-consumer';
import { waRequest } from './wa-requests-consumer';
import { zipFolder } from './zip-folder-consumer';
import { notification } from './notification-consumer';

export interface IConsumer {
    queue: string,
    processor: Function
}


export const mailRequestsConsumer: IConsumer = mailRequests;
export const mailReportsConsumer: IConsumer = mailReports;

// Mail Event Consumer
export const mailEventsConsumer: IConsumer = mailEvent;

export const waReportsConsumer: IConsumer = waReport;
export const waRequestsConsumer: IConsumer = waRequest;
export const zipFolderConsumer: IConsumer = zipFolder;
export const notificationConsumer: IConsumer = notification;



