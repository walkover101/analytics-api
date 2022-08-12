import { getHashCode } from "../services/utility-service";

export default class MailEvent {
    requestId: string; //requestId of the mail. (Not unique in this table)	
    eventId: number; //Events that are initiated by the recipient of the mail. (5,6,7,10)	
    recipientEmail: string; //Recipient Email Address
    outboundEmailId: number; //Unique Id is generated for each request (All recipients, cc, bcc of that mail will have same id) in MySQL.
    companyId: string; //Id of the company which requested this mail.	
    requestTime: Date; //Timestamp when this mail was requested to be sent.	
    createdAt: Date; //Timestamp when event occurred.	

    constructor(attr: any) {
        this.eventId = parseInt(attr['eid']);

        //common in all three email models
        this.recipientEmail = attr['rem']?.toLowerCase();
        this.outboundEmailId = parseInt(attr['oid']);
        this.requestId = getHashCode(`${this.outboundEmailId}-${this.recipientEmail}`);
        this.companyId = attr['cid'];
        this.requestTime = attr['mct'] && new Date(attr['mct']);
        this.createdAt = attr['created_at'] && new Date(attr['created_at']);
    }
}