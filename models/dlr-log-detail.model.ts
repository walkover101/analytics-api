export default class DlrLogDetail {
    id: string;
    eventId: number;
    uid: string;
    createdAt: Date;

    constructor(attr: any) {
        this.id = attr['_id'];
        this.eventId = parseInt(attr['eid']);
        this.uid = attr['uid'];
        this.createdAt = attr['cat']?.$date?.$numberLong && new Date(parseFloat(attr['cat']?.$date?.$numberLong));
    }
}