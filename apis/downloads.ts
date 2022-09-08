import express from 'express';
import * as downloadsController from '../controllers/downloads';

const router = express.Router();

// POST '/exports/sms' | '/exports/otp' | '/exports/mail' | '/exports/wa'
router.post(/^\/(sms|otp|mail|wa)/, downloadsController.downloadCsv);

// GET '/exports' | '/exports/sms' | '/exports/otp' | '/exports/mail' | '/exports/wa'
router.get(/^\/($|sms|otp|mail|wa)/, downloadsController.getDownloadLinks);

export default router;