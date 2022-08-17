import express from 'express';
import * as downloadsController from '../controllers/downloads';

const router = express.Router();

router.post(/^\/(sms|email)/, downloadsController.downloadCsv);

router.get(/^\/(sms|email)/, downloadsController.getDownloadLinks);

export default router;