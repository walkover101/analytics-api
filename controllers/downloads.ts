import { Request, Response } from "express";
import logger from '../logger/logger';
import { formatDate } from "../services/utility-service";
import Download, { DOWNLOAD_STATUS } from '../models/download.model';
import { REPORT_TYPE } from '../models/download.model';
import rabbitmqProducer from "../database/rabbitmq-producer";

const DEFAULT_PAGE_SIZE = +(process.env.DEFAULT_PAGE_SIZE || 30);
const ZIP_FOLDER_QUEUE = process.env.RABBIT_ZIP_FOLDER_QUEUE_NAME || 'zip-folder';

// POST '/exports/sms' | '/exports/otp' | '/exports/mail' | '/exports/wa'
const downloadCsv = (reportType: REPORT_TYPE) => {
    return async (req: Request, res: Response) => {
        try {
            const { companyId, fields, timezone, email } = req.query;
            const resourceType = req.params[0]; // sms or otp or mail or wa
            const startDate = formatDate(req.query.startDate as string);
            const endDate = formatDate(req.query.endDate as string);
            if (!companyId) throw 'Company Id is mandatory';
            const download = new Download(reportType, resourceType as string, companyId as string, startDate, endDate, timezone as string, email as string, fields as string, req.query);
            const downloadDoc = await download.save();
            download.id = downloadDoc.id;
            res.send({ status: DOWNLOAD_STATUS.PROCESSING, message: 'Request Accepted. Please check back after few minutes.' });

            try {
                const [exportJob] = await download.createJob();
                download.update({ status: DOWNLOAD_STATUS.PROCESSING });
                await exportJob.getQueryResults();
                rabbitmqProducer.publishToQueue(ZIP_FOLDER_QUEUE, download.zipInfo)
            } catch (err: any) {
                download.update({ status: DOWNLOAD_STATUS.ERROR, err: err.message });
                console.error(err);
            }
        } catch (error: any) {
            logger.error(error);
            res.status(400).send({ status: DOWNLOAD_STATUS.ERROR, error });
        }
    }
}

// GET '/exports' | '/exports/sms' | '/exports/otp' | '/exports/mail' | '/exports/wa'
const getDownloadLinks = (reportType: REPORT_TYPE) => {
    return async (req: Request, res: Response) => {
        try {
            let { companyId, resourceType, page, pageSize } = req.query;
            const pageNumber = page ? +page : 1;
            const offset = pageSize ? +pageSize : DEFAULT_PAGE_SIZE;
            resourceType = req.params[0] || resourceType; // Pick from path param else from query
            logger.info(`[DOWNLOAD](companyId: ${companyId} | reportType: logs | resourceType: ${resourceType}) Fetching records...`);
            const result = await Download.index(reportType, pageNumber, offset, companyId as string, resourceType as string);
            res.send(result);
        } catch (err: any) {
            logger.error(err);
            res.status(500).send({ "error": err.message });
        }
    }
}

export {
    downloadCsv,
    getDownloadLinks
};