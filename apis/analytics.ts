import express from 'express';
import * as analyticsController from '../controllers/analytics';

const router = express.Router();

// GET '/analytics/sms'
router.get('/sms', analyticsController.getSmsAnalytics);

// GET '/analytics/mail'
router.get('/mail', analyticsController.getMailAnalytics);

// GET '/analytics/otp'
router.get('/otp', analyticsController.getOtpAnalytics);

// GET '/analytics/campaigns'
router.get('/campaigns', analyticsController.getCampaignAnalytics);

export default router;