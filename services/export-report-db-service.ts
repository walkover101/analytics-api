import { CollectionReference } from 'firebase-admin/firestore';
import { db } from '../firebase';

const EXPORT_REPORT_COLLECTION = process.env.EXPORT_REPORT_COLLECTION || 'exports'

class ExportReportDBService {
    private static instance: ExportReportDBService;
    private collection: CollectionReference;

    constructor() {
        this.collection = db.collection(EXPORT_REPORT_COLLECTION);
    }

    public static getSingletonInstance(): ExportReportDBService {
        return ExportReportDBService.instance ||= new ExportReportDBService();
    }

    public insert(doc: any) {
        return this.collection.add(doc);
    }
}

export default ExportReportDBService.getSingletonInstance();
