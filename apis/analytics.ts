import express from 'express';
import * as analyticsController from '../controllers/analytics';
import * as downloadsController from '../controllers/downloads';
import mailDomainService, * as MailDomainService from '../services/email/mail-domain-service';
import { REPORT_TYPE } from '../models/download.model';

const router = express.Router();

// GET '/analytics/campaigns'
router.get('/campaigns', analyticsController.getCampaignAnalytics);

// GET 'analytics/mail/domain'
router.get('/mail/domain', analyticsController.getMailDomain);

// GET '/analytics/sms' | '/analytics/mail' | '/analytics/otp' | '/analytics/wa' | '/analytics/voice'
router.get(/^\/(sms|otp|mail|wa|voice)/, analyticsController.getAnalytics);

// POST '/analytics/sms/export' | '/analytics/mail/export' | '/analytics/otp/export' | '/analytics/wa/export' | '/analytics/voice/export'
router.post(/^\/(sms|mail|otp|wa|voice)\/export\b/, downloadsController.downloadCsv(REPORT_TYPE.ANALYTICS));

// GET '/analytics/sms/export' | '/analytics/mail/export' | '/analytics/otp/export' | '/analytics/wa/export' | '/analytics/voice/export'
router.get(/^\/(sms|mail|otp|wa|voice)\/export\b/, downloadsController.getDownloadLinks(REPORT_TYPE.ANALYTICS));

// GET '/analytics/export' | '/analytics/export'
router.get('/export', downloadsController.getDownloadLinks(REPORT_TYPE.ANALYTICS));

export default router;