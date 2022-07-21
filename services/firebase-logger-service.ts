import { CollectionReference } from 'firebase-admin/firestore';
import { db } from '../firebase';
import logger from '../logger/logger';
import { jobType } from '../models/trackers.model';

const REQUEST_SYNC_ERROR_COLLECTION = process.env.REQUEST_SYNC_ERROR_COLLECTION || 'requestSyncJobErrors'
const REPORT_SYNC_ERROR_COLLECTION = process.env.REPORT_SYNC_ERROR_COLLECTION || 'reportSyncJobErrors'

class ErrorTrackerService {
    private static instance: ErrorTrackerService;
    private requestErrCollection: CollectionReference;
    private reportErrCollection: CollectionReference;

    constructor() {
        this.requestErrCollection = db.collection(REQUEST_SYNC_ERROR_COLLECTION);
        this.reportErrCollection = db.collection(REPORT_SYNC_ERROR_COLLECTION);
    }

    public static getSingletonInstance(): ErrorTrackerService {
        return ErrorTrackerService.instance ||= new ErrorTrackerService();
    }

    public logToFirebase(job: jobType, errors: any) {
        logger.error('[ERROR] Logging errors in firestore...');
        const batch = db.batch();
        let collection = job === jobType.REPORT_DATA ? this.reportErrCollection : this.requestErrCollection;
        errors.forEach((error: any) => batch.set(collection.doc(), error));
        return batch.commit();
    }
}

export default ErrorTrackerService.getSingletonInstance().logToFirebase;
