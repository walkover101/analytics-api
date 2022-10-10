import express from 'express';
import * as data from '../controllers/data-integrity';

const router = express.Router();

// GET '/'
router.get('/', data.integrityCheck);


export default router;