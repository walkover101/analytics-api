import express from 'express';
import * as analyticsController from '../controllers/analytics';
import * as downloadsController from '../controllers/downloads';
import { REPORT_TYPE } from '../models/download.model';

const router = express.Router();

// GET '/analytics/campaigns'
router.get('/campaigns', analyticsController.getCampaignAnalytics);

// GET '/analytics/sms' | '/analytics/mail' | '/analytics/otp' | '/analytics/wa'
router.get(/^\/(sms|otp|mail|wa|voice)/, analyticsController.getAnalytics);

// POST '/analytics/sms/export' | '/analytics/mail/export' | '/analytics/otp/export' | '/analytics/wa/export'
router.post(/^\/(sms|mail|otp|wa|voice)\/export\b/, downloadsController.downloadCsv(REPORT_TYPE.ANALYTICS));

// GET '/analytics/sms/export' | '/analytics/mail/export' | '/analytics/otp/export' | '/analytics/wa/export'
router.get(/^\/(sms|mail|otp|wa|voice)\/export\b/, downloadsController.getDownloadLinks(REPORT_TYPE.ANALYTICS));

// GET '/analytics/export' | '/analytics/export'
router.get('/export', downloadsController.getDownloadLinks(REPORT_TYPE.ANALYTICS));

export default router;