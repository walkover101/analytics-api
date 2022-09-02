import express from 'express';
import * as logsController from '../controllers/logs';

const router = express.Router();

// GET '/logs/sms'
router.get('/sms', logsController.getSmsLogs);

// GET '/logs/otp'
router.get('/otp', logsController.getOtpLogs);

// GET '/logs/mail'
router.get('/mail', logsController.getMailLogs);

// GET '/logs/mail/:requestId'
router.get('/mail/:requestId', logsController.getMailLogDetails);

export default router;