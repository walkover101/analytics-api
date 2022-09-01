import express from 'express';
import * as homeController from '../controllers/home';

const router = express.Router();

// GET '/'
router.get('/', homeController.healthcheck);

// POST '/'
router.post('/', homeController.test);

export default router;