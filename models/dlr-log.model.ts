export default class DlrLog {
    private id: string;
    private senderDedicatedIpId: number;
    private statusCode: number;
    private enhancedStatusCode: string;
    private resultState: string;
    private reason: string;
    private remoteMx: string;
    private remoteIp: string;
    private contentSize: number;
    private diffDeliveryTime: number;
    private openCount: number;
    private hostname: string;
    private outboundEmailId: number;
    private recipientEmail: string;
    private eventId: number;
    private uid: string;
    private companyId: number;
    private subject: string;
    private domain: string;
    private senderEmail: string;
    private mailTypeId: number;
    private templateSlug: string;
    private mailerRequestId: string;
    private campaignId: number;
    private clientRequestIp: string;
    private createdAt: Date;
    private updatedAt: Date;

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