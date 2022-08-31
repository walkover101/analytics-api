import express from 'express';
import * as profitsController from '../controllers/profits';

const router = express.Router();

// GET '/profits/vendors'
router.get('/vendors', profitsController.getVendorProfits);

// GET '/profits/sms'
router.get('/sms', profitsController.getSmsProfits);

export default router;