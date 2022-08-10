export default class MailEvent {
    requestId: string; //requestId of the mail. (Not unique in this table)	
    eventId: number; //Events that are initiated by the recipient of the mail. (5,6,7,10)	
    createdAt: Date; //Timestamp when event occurred.	
    requestTime: Date; //Timestamp when this mail was requested to be sent.	
    companyId: string; //Id of the company which requested this mail.	

    constructor(attr: any) {
        this.requestId = attr['_id'];
        this.eventId = parseInt(attr['eid']);
        this.createdAt = attr['created_at'] && new Date(attr['created_at']);
        this.requestTime = attr['mct']?.$date?.$numberLong && new Date(parseFloat(attr['mct']?.$date?.$numberLong));
        this.companyId = attr['cid'];
    }
}