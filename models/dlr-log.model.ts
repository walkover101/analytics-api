export default class DlrLog {
    id: string;
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
    outboundEmailId: number;
    recipientEmail: string;
    eventId: number;
    uid: string;
    companyId: number;
    subject: string;
    domain: string;
    senderEmail: string;
    mailTypeId: number;
    templateSlug: string;
    mailerRequestId: string;
    campaignId: number;
    clientRequestIp: string;
    createdAt: Date;
    updatedAt: Date;

    constructor(attr: any) {
        this.id = attr['_id'];
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
        this.outboundEmailId = parseInt(attr['oid']);
        this.recipientEmail = attr['rem'];
        this.eventId = parseInt(attr['eid']);
        this.uid = attr['uid'];
        this.companyId = parseInt(attr['cid']);
        this.subject = attr['sub'];
        this.domain = attr['dmn'];
        this.senderEmail = attr['sem'];
        this.mailTypeId = parseInt(attr['mti']);
        this.templateSlug = attr['tnm'];
        this.mailerRequestId = attr['mri'];
        this.campaignId = parseInt(attr['cmp']);
        this.clientRequestIp = attr['cri'];
        this.createdAt = attr['cat']?.$date?.$numberLong && new Date(parseFloat(attr['cat']?.$date?.$numberLong));
        this.updatedAt = attr['uat']?.$date?.$numberLong && new Date(parseFloat(attr['uat']?.$date?.$numberLong));
    }
}