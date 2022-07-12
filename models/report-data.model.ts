export default class ReportData {
    private '_id': string;
    private 'requestID': string;
    private 'telNum': string;
    private 'status': number;
    private 'sentTime': Date;
    private 'providerSMSID': string;
    private 'user_pid': string;
    private 'senderID': string;
    private 'smsc': string;
    private 'deliveryTime': Date;
    private 'route': string;
    private 'credit': number;
    private 'retryCount': number;
    private 'sentTimePeriod': Date;
    private 'crcy': string;
    private 'node_id': string;
    private 'oppri': number;
    private 'isSingleRequest': string;

    constructor(attr: any) {
        this._id = attr['_id'].toString();
        this.requestID = attr['requestID'];
        this.telNum = attr['telNum'];
        this.status = parseInt(attr['status']);
        this.sentTime = attr['sentTime'];
        this.providerSMSID = attr['providerSMSID'];
        this.user_pid = attr['user_pid'];
        this.senderID = attr['senderID'];
        this.smsc = attr['smsc'];
        this.deliveryTime = attr['deliveryTime'];
        this.route = attr['route'];
        this.credit = parseFloat(attr['credit']);
        this.retryCount = parseInt(attr['retryCount']);
        this.sentTimePeriod = attr['sentTimePeriod'];
        this.crcy = attr['crcy'];
        this.node_id = attr['node_id'];
        this.oppri = parseFloat(attr['oppri'] || 0);
        this.isSingleRequest = attr['isSingleRequest'];
    }
}