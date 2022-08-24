import express from 'express';
import * as logsController from '../controllers/logs';

const router = express.Router();

router.get('/mail', logsController.getMailLogs);

export default router;