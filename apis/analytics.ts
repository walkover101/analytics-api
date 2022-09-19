import express from 'express';
import * as analyticsController from '../controllers/analytics';
import * as downloadsController from '../controllers/downloads';
import { REPORT_TYPE } from '../models/download.model';

const router = express.Router();

// GET '/analytics/sms'
router.get('/sms', analyticsController.getSmsAnalytics);

// GET '/analytics/mail'
router.get('/mail', analyticsController.getMailAnalytics);

// GET '/analytics/otp'
router.get('/otp', analyticsController.getOtpAnalytics);

// GET '/analytics/wa'
router.get('/wa', analyticsController.getWaAnalytics);

// GET '/analytics/campaigns'
router.get('/campaigns', analyticsController.getCampaignAnalytics);

// POST '/analytics/sms/export' | '/analytics/mail/export' | '/analytics/otp/export' | '/analytics/wa/export'
router.post(/^\/(sms|mail|otp|wa)\/export\b/, downloadsController.downloadCsv(REPORT_TYPE.ANALYTICS));

// GET '/analytics/sms/export' | '/analytics/mail/export' | '/analytics/otp/export' | '/analytics/wa/export'
router.get(/^\/(sms|mail|otp|wa)\/export\b/, downloadsController.getDownloadLinks(REPORT_TYPE.ANALYTICS));

// GET '/analytics/export' | '/analytics/export'
router.get('/export', downloadsController.getDownloadLinks(REPORT_TYPE.ANALYTICS));

export default router;