import { mailEvent } from './mail-events-consumer';
import { mailRequests } from './mail-requests-consumer';
import { mailReports } from './mail-reports-consumer';
import { waReport } from './wa-reports-consumer';
import { waRequest } from './wa-requests-consumer';
import { zipFolder } from './zip-folder-consumer';
import { notification } from './notification-consumer';
import { voiceReports } from './voice-report-consumer';
import { voiceRequests } from './voice-request-consumer';

export interface IConsumer {
    queue: string,
    processor: Function,
    prefetch: number
}

// Voice Consumers
export const voiceRequestsConsumer: IConsumer = voiceRequests;
export const voiceReportsConsumer: IConsumer = voiceReports;

// Mail Consumers
export const mailRequestsConsumer: IConsumer = mailRequests;
export const mailReportsConsumer: IConsumer = mailReports;
export const mailEventsConsumer: IConsumer = mailEvent;

// Whatsapp Consumers
export const waReportsConsumer: IConsumer = waReport;
export const waRequestsConsumer: IConsumer = waRequest;


// Other Consumers
export const zipFolderConsumer: IConsumer = zipFolder;
export const notificationConsumer: IConsumer = notification;



