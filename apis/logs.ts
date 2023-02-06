import express from 'express';
import * as logsController from '../controllers/logs';

const router = express.Router();

//GET '/logs/sms' | 'logs/mail' | 'logs/otp' | 'logs/wa' | 'logs/voice'
router.get(/^\/(sms|otp|mail|wa|voice)/, logsController.getLogs)

// GET '/logs/mail/:requestId'
router.get('/mail/:requestId', logsController.getMailLogDetails);

export default router;