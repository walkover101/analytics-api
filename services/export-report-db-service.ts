import { CollectionReference } from 'firebase-admin/firestore';
import { db } from '../firebase';
import ExportReport from '../models/export-report.model';

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

    public insert(doc: ExportReport) {
        return this.collection.add(JSON.parse(JSON.stringify(doc)));
    }

    public update(docId: string, params: any) {
        let { status, files, err } = params;
        const data: any = {};
        if (status) data.status = status;
        if (files) data.files = files;
        if (err) data.err = err;
        data.updatedAt = new Date().toISOString();
        return this.collection.doc(docId).update(data);
    }

    public index(companyId?: string) {
        if (companyId) {
            return this.collection.where('companyId', '==', companyId).get();
        }

        return this.collection.get();
    }
}

export default ExportReportDBService.getSingletonInstance();
