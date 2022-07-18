import express, { Request, Response } from 'express';
import logger from '../logger/logger';
import exportService from '../services/export-report-db-service';
import reportDataService from '../services/sms/report-data-service';
import ExportReport, { EXPORT_STATUS } from '../models/export-report.model';
import { formatDate } from '../services/utility-service';

const router = express.Router();

router.route('/').post(async (req: Request, res: Response) => {
    try {
        let { companyId, route, fields } = req.query;
        let startDate = formatDate(req.query.startDate as string);
        let endDate = formatDate(req.query.endDate as string);
        if (!startDate) return res.status(400).send({ message: 'Start Date must be provided in MM-DD-YYYY format' });
        if (!endDate) return res.status(400).send({ message: 'End Date must be provided in MM-DD-YYYY format' });
        if (!companyId) return res.status(400).send({ message: 'Company Id is mandatory' });
        logger.info('[EXPORT] Creating entry in firestore...');
        const exportReport = new ExportReport(companyId as string, startDate, endDate, fields as string, route as string);
        const exportReportDoc = await exportService.insert(exportReport);
        logger.info('[EXPORT] Sending response to client...');
        res.send({ id: exportReportDoc.id });

        try {
            logger.info('[EXPORT] Creating job...');
            const [exportJob] = await reportDataService.export(exportReportDoc.id, exportReport);
            logger.info('[EXPORT] Job created. Processing query...');
            exportService.update(exportReportDoc.id, { status: EXPORT_STATUS.PROCESSING });
            await exportJob.getQueryResults();
            logger.info('[EXPORT] Export completed.');
            exportService.update(exportReportDoc.id, { status: EXPORT_STATUS.SUCCESS, files: [exportReportDoc.id] });
        } catch (err: any) {
            exportService.update(exportReportDoc.id, { status: EXPORT_STATUS.ERROR, err: err.message });
            logger.error(err);
        }
    } catch (err: any) {
        logger.error(err);
        res.status(500).send({ "error": err.message });
    }
});

router.route('/').get(async (req: Request, res: Response) => {
    try {
        let { companyId } = req.query;
        logger.info(`[EXPORT](companyId: ${companyId}) Fetching records...`);
        const snapshot = await exportService.index(companyId as string);
        const docs = snapshot.docs;
        const results = docs.map(doc => {
            const document = doc.data();
            document.id = doc.id;
            return document;
        });
        res.send(results);
    } catch (err: any) {
        logger.error(err);
        res.status(500).send({ "error": err.message });
    }
});
export default router;