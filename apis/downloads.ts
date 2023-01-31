import express from 'express';
import * as downloadsController from '../controllers/downloads';
import { REPORT_TYPE } from '../models/download.model';

const router = express.Router();

// POST '/exports/sms' | '/exports/otp' | '/exports/mail' | '/exports/wa'
router.post(/^\/(sms|otp|mail|wa|voice)/, downloadsController.downloadCsv(REPORT_TYPE.LOGS));

// GET '/exports' | '/exports/sms' | '/exports/otp' | '/exports/mail' | '/exports/wa'
router.get(/^\/($|sms|otp|mail|wa|voice)/, downloadsController.getDownloadLinks(REPORT_TYPE.LOGS));

export default router;