export default class MailRequest {
    id: string;
    companyId: number;
    subject: string;
    domain: string;
    senderEmail: string;
    recipientEmail: string;
    outboundEmailId: number;
    mailTypeId: number;
    templateSlug: string;
    mailerRequestId: string;
    nodeId: number;
    clientRequestIP: string;
    createdAt: Date;

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