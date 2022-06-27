import express, { Request, Response } from 'express';
import logger from '../logger/logger';
const router = express.Router();
router.route('/')
    .get((req: Request, res: Response) => {
        res.send("Work in progress...");
    })
export default router;