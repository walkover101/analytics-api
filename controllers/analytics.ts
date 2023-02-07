import { Request, Response } from "express";
import logger from '../logger/logger';
import smsAnalyticsService from "../services/sms/sms-analytics-service";
import otpAnalyticsService from "../services/otp/otp-analytics-service";
import mailAnalyticsService from "../services/email/mail-analytics-service";
import { formatDate, getDefaultDate } from "../services/utility-service";
import waAnalyticsService from "../services/whatsapp/wa-analytics-service";
import voiceAnalyticsService from "../services/voice/voice-analytics-service";
import { RESOURCE_TYPE } from "../models/download.model";
import mailDomainService from "../services/email/mail-domain-service";

// GET '/analytics/sms' | '/analytics/mail' | '/analytics/otp' | '/analytics/wa'
const getAnalytics = async (req: Request, res: Response) => {
    try {
        const params = { ...req.query, ...req.params } as any;
        const resourceType = req.params[0]; // sms or otp or mail or wa
        let { companyId, timeZone, groupBy, startDate = getDefaultDate().from, endDate = getDefaultDate().to } = params;
        const fromDate = formatDate(startDate);
        const toDate = formatDate(endDate);

        const analytics = await getService(resourceType)?.getAnalytics(companyId, fromDate, toDate, timeZone, params, groupBy);
        res.send(analytics);
    } catch (error: any) {
        logger.error(error);
        res.status(400).send({ error: error?.message || error });
    }
}

// GET '/analytics/campaigns'
const getCampaignAnalytics = async (req: Request, res: Response) => {
    try {
        const params = { ...req.query, ...req.params } as any;
        let { companyId, smsNodeIds, smsReqIds, waNodeIds, waReqIds, emailNodeIds, emailReqIds, allNodes, timeZone, groupBy, mailGroupBy, startDate = getDefaultDate().from, endDate = getDefaultDate().to } = params;
        const fromDate = formatDate(startDate);
        const toDate = formatDate(endDate);
        if (!companyId) throw "companyId required";
        let smsAnalytics, waAnalytics, mailAnalytics;
        if (allNodes == 'true') {
            smsAnalytics = await smsAnalyticsService.getAnalytics(companyId, fromDate, toDate, timeZone, params, groupBy, true);
            mailAnalytics = await mailAnalyticsService.getAnalytics(companyId, fromDate, toDate, timeZone, params, mailGroupBy, true);
            waAnalytics = await waAnalyticsService.getAnalytics(companyId, fromDate, toDate, timeZone, params, groupBy, true);
        } else if (!smsNodeIds?.length && !smsReqIds?.length && !waNodeIds?.length && !waReqIds?.length && !emailNodeIds?.length && !emailReqIds?.length) {
            smsAnalytics = { data: [], total: {} };
            mailAnalytics = { data: [], total: {} };
            waAnalytics = { data: [], total: {} };
        } else {
            if (smsNodeIds?.length || smsReqIds?.length) smsAnalytics = await smsAnalyticsService.getAnalytics(companyId, fromDate, toDate, timeZone, params, groupBy);
            if (waNodeIds?.length || waReqIds?.length) waAnalytics = await waAnalyticsService.getAnalytics(companyId, fromDate, toDate, timeZone, params, groupBy);
            if (emailNodeIds?.length || emailReqIds?.length) mailAnalytics = await mailAnalyticsService.getAnalytics(companyId, fromDate, toDate, timeZone, params, mailGroupBy);
        }

        res.send({ sms: smsAnalytics, mail: mailAnalytics, wa: waAnalytics });
    } catch (error: any) {
        logger.error(error);
        res.status(400).send({ error: error?.message || error });
    }
}

    // GET 'mail/domain'
    const getMailDomain = async ( req: Request, res: Response) => {
        try {
            const params = { ...req.query, ...req.params } as any;
            let { companyId, startDate = getDefaultDate().from, endDate = getDefaultDate().to, timeZone } = params;
            const fromDate = formatDate(startDate);
            const toDate = formatDate(endDate);
    
            const analytics = await mailDomainService.getAnalytics(companyId, fromDate, toDate, timeZone);
            res.send(analytics);
        } catch (error: any) {
            logger.error(error);
            res.status(400).send({ error: error?.message || error });
        }
    }

const getService = (resourceType: string) => {
    try {
        switch (resourceType) {
            case RESOURCE_TYPE.EMAIL:
                return mailAnalyticsService;
            case RESOURCE_TYPE.OTP:
                return otpAnalyticsService;
            case RESOURCE_TYPE.WA:
                return waAnalyticsService;
            case RESOURCE_TYPE.VOICE:
                return voiceAnalyticsService;
            default:
                return smsAnalyticsService;
        }
    } catch (error: any) {
        logger.error(error);
    }
}

export {
    getCampaignAnalytics,
    getAnalytics,
    getMailDomain
};