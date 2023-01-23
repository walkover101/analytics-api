import { Request, Response } from "express";
import logger from '../logger/logger';
import mailLogsService from "../services/email/mail-logs-service";
import smsLogsService from "../services/sms/sms-logs-service";
import { formatDate, getDefaultDate } from "../services/utility-service";
import MailEvent from '../models/mail-event.model';
import otpLogsService from "../services/otp/otp-logs-service";
import waLogsService from "../services/whatsapp/wa-logs-service";
import { RESOURCE_TYPE } from "../models/download.model";
import voiceLogsService from "../services/voice/voice-logs-service";

//GET '/logs/sms' | 'logs/mail' | 'logs/otp' | 'logs/wa'
const getLogs = async (req: Request, res: Response) => {
    try {
        const params = { ...req.query, ...req.params } as any;
        const resourceType = req.params[0]; // sms | otp | mail | wa
        let { companyId, timeZone, fields, startDate = getDefaultDate().from, endDate = getDefaultDate().to } = params;
        const fromDate = formatDate(startDate);
        const toDate = formatDate(endDate);
        const attributes = fields?.length ? fields.splitAndTrim(',') : [];

        const logs = await getService(resourceType)?.getLogs(companyId, fromDate, toDate, timeZone, params, attributes, {
            paginationToken: params?.paginationToken,
            offset: params?.offset,
            limit: params?.limit
        });
        res.send(logs);
    } catch (error: any) {
        logger.error(error);
        res.status(400).send({ error: error?.message || error });
    }
}

// GET '/logs/mail/:requestId'
const getMailLogDetails = async (req: Request, res: Response) => {
    try {
        const params = { ...req.query, ...req.params } as any;
        let { companyId, requestId, fields } = params;
        const attributes = fields?.length ? fields.splitAndTrim(',') : [];
        if (!requestId) throw "requestId required";

        const logs = await MailEvent.index(companyId, requestId, params, attributes);
        res.send({ data: logs });
    } catch (error: any) {
        logger.error(error);
        res.status(400).send({ error: error?.message || error });
    }
}

const getService = (resourceType: string) => {
    try {
        switch (resourceType) {
            case RESOURCE_TYPE.EMAIL:
                return mailLogsService;
            case RESOURCE_TYPE.OTP:
                return otpLogsService;
            case RESOURCE_TYPE.WA:
                return waLogsService;
            case RESOURCE_TYPE.VOICE:
                return voiceLogsService;
            default:
                return smsLogsService;
        }
    } catch (error: any) {
        logger.error(error);
    }
}

export {
    getMailLogDetails,
    getLogs
};