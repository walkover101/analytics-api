export default class MailRequest {
    id: string; //Unique for each mail request. (outboundEmailId + recipientEmail)
    companyId: number; //Id of the company which requested this mail.
    subject: string; //Email Subject
    domain: string; //Email Domain: walkover.in
    senderEmail: string; //Sender Email Address
    recipientEmail: string; //Recipient Email Address
    outboundEmailId: number; //Unique Id is generated for each request (All recipients, cc, bcc of that mail will have same id) in MySQL.
    mailTypeId: number; //1 =>Transactional, 2 =>Notification, 3 =>Promotional
    templateSlug: string; //Id of email template
    mailerRequestId: string;
    nodeId: number; //Node Id used in campaign
    clientRequestIP: string; //Client IP
    createdAt: Date; //Timestamp when this mail was requested to be sent.

    senderDedicatedIpId: number;
    statusCode: number;
    enhancedStatusCode: string;
    resultState: string;
    reason: string;
    remoteMx: string;
    remoteIp: string;
    contentSize: number;
    diffDeliveryTime: number;
    openCount: number; // 0 always, calculate from dlr log details
    hostname: string;
    eventId: number;
    uid: string;
    updatedAt: Date;

    constructor(attr: any) {
        this.id = attr['_id'];
        this.companyId = parseInt(attr['cid']);
        this.subject = attr['sub'];
        this.domain = attr['dmn'];
        this.senderEmail = attr['sem'];
        this.recipientEmail = attr['rem'];
        this.outboundEmailId = parseInt(attr['oid']);
        this.mailTypeId = parseInt(attr['mti']);
        this.templateSlug = attr['tnm'];
        this.mailerRequestId = attr['mri'];
        this.nodeId = parseInt(attr['cmp']);
        this.clientRequestIP = attr['cri'];
        this.createdAt = attr['created_at'] && new Date(attr['created_at']);

        this.senderDedicatedIpId = parseInt(attr['sid']);
        this.statusCode = parseInt(attr['stc']);
        this.enhancedStatusCode = attr['esc'];
        this.resultState = attr['rst'];
        this.reason = attr['rsn'];
        this.remoteMx = attr['rmx'];
        this.remoteIp = attr['rip'];
        this.contentSize = parseInt(attr['csz']);
        this.diffDeliveryTime = parseFloat(attr['ddt']);
        this.openCount = parseInt(attr['oct'] || 0);
        this.hostname = attr['hnm'];
        this.eventId = parseInt(attr['eid']);
        this.uid = attr['uid'];
        this.updatedAt = attr['uat']?.$date?.$numberLong && new Date(parseFloat(attr['uat']?.$date?.$numberLong));
    }
}