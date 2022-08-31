import express from 'express';
import * as downloadsController from '../controllers/downloads';

const router = express.Router();

// POST '/exports/sms' | '/exports/otp' | '/exports/mail'
router.post(/^\/(sms|otp|mail)/, downloadsController.downloadCsv);

// GET '/exports' | '/exports/sms' | '/exports/otp' | '/exports/mail'
router.get(/^\/($|sms|otp|mail)/, downloadsController.getDownloadLinks);

export default router;