export default class DlrLogDetail {
    id: string;
    eventId: number;
    uid: string;
    createdAt: Date;
    mailCreationTime: Date;

    constructor(attr: any) {
        this.id = attr['_id'];
        this.eventId = parseInt(attr['eid']);
        this.uid = attr['uid'];
        this.mailCreationTime = attr['mct']?.$date?.$numberLong && new Date(parseFloat(attr['mct']?.$date?.$numberLong));
        this.createdAt = attr['cat']?.$date?.$numberLong && new Date(parseFloat(attr['cat']?.$date?.$numberLong));
    }
}