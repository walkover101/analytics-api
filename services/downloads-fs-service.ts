import { CollectionReference } from 'firebase-admin/firestore';
import { db } from '../firebase';
import Download from '../models/download.model';

const DOWNLOADS_COLLECTION = process.env.DOWNLOADS_COLLECTION || 'downloads'

class DownloadsFsService {
    private static instance: DownloadsFsService;
    private collection: CollectionReference;

    constructor() {
        this.collection = db.collection(DOWNLOADS_COLLECTION);
    }

    public static getSingletonInstance(): DownloadsFsService {
        return DownloadsFsService.instance ||= new DownloadsFsService();
    }

    public insert(doc: Download) {
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

export default DownloadsFsService.getSingletonInstance();
