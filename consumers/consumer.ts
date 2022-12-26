import { mailEvent } from './mail-events-consumer';
import { mailRequests } from './mail-requests-consumer';
import { mailReports } from './mail-reports-consumer';
import { waReport } from './wa-reports-consumer';
import { waRequest } from './wa-requests-consumer';
import { zipFolder } from './zip-folder-consumer';
import { notification } from './notification-consumer';
import { voiceReports } from './voice-report-consumer';
import { voiceRequests } from './voice-request-consumer';

export interface Consumer {
    queue: string,
    processor: Function
}


export const mailRequestsConsumer: Consumer = mailRequests;
export const mailReportsConsumer: Consumer = mailReports;

// Mail Event Consumer
export const mailEventsConsumer: Consumer = mailEvent;

export const waReportsConsumer: Consumer = waReport;
export const waRequestsConsumer: Consumer = waRequest;
export const zipFolderConsumer: Consumer = zipFolder;
export const notificationConsumer: Consumer = notification;
export const voiceReportsConsumer: Consumer = voiceReports;
export const voiceRequestsConsumer: Consumer = voiceRequests;