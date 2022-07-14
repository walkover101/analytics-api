import express, { Request, Response } from 'express';
import logger from '../logger/logger';
import exportService from '../services/export-report-db-service';
import { DateTime } from 'luxon';

const router = express.Router();

router.route('/').get(async (req: Request, res: Response) => {
    try {
        const doc = await exportService.insert({
            companyId: 'COMAPNY_ID_1',
            startDate: DateTime.now().minus({ minutes: 48 * 60 }),
            endDate: DateTime.now(),
            route: 1,
            downloadLinks: ['http://google.com']
        });

        res.send(doc.id);
    } catch (err: any) {
        logger.error(err);
        res.status(500).send({ "error": err.message });
    }
});

export default router;