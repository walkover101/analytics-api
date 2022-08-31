import express from 'express';
import * as downloadsController from '../controllers/downloads';

const router = express.Router();

router.post(/^\/(sms|otp|mail)/, downloadsController.downloadCsv);

router.get('/', downloadsController.getDownloadLinks);

router.get(/^\/(sms|otp|mail)/, downloadsController.getDownloadLinks);

export default router;