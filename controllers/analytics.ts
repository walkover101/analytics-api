import { Request, Response } from "express";
import logger from '../logger/logger';
import smsAnalyticsService from "../services/sms/sms-analytics-service";
import otpAnalyticsService from "../services/otp/otp-analytics-service";
import mailAnalyticsService from "../services/email/mail-analytics-service";
import { formatDate, getDefaultDate } from "../services/utility-service";
import waAnalyticsService from "../services/whatsapp/wa-analytics-service";

// GET '/analytics/sms'
const getSmsAnalytics = async (req: Request, res: Response) => {
    try {
        const params = { ...req.query, ...req.params } as any;
        let { companyId, timeZone, groupBy, startDate = getDefaultDate().from, endDate = getDefaultDate().to } = params;
        const fromDate = formatDate(startDate);
        const toDate = formatDate(endDate);

        const analytics = await smsAnalyticsService.getAnalytics(companyId, fromDate, toDate, timeZone, params, groupBy);
        res.send(analytics);
    } catch (error: any) {
        logger.error(error);
        res.status(400).send({ error: error?.message || error });
    }
}

// GET '/analytics/mail'
const getMailAnalytics = async (req: Request, res: Response) => {
    try {
        const params = { ...req.query, ...req.params } as any;
        let { companyId, timeZone, groupBy, startDate = getDefaultDate().from, endDate = getDefaultDate().to } = params;
        const fromDate = formatDate(startDate);
        const toDate = formatDate(endDate);
        if (!companyId) throw "companyId required";

        const analytics = await mailAnalyticsService.getAnalytics(companyId, fromDate, toDate, timeZone, params, groupBy);
        res.send(analytics);
    } catch (error: any) {
        logger.error(error);
        res.status(400).send({ error: error?.message || error });
    }
}

// GET '/analytics/otp'
const getOtpAnalytics = async (req: Request, res: Response) => {
    try {
        const params = { ...req.query, ...req.params } as any;
        let { companyId, timeZone, groupBy, startDate = getDefaultDate().from, endDate = getDefaultDate().to } = params;
        const fromDate = formatDate(startDate);
        const toDate = formatDate(endDate);

        const analytics = await otpAnalyticsService.getAnalytics(companyId, fromDate, toDate, timeZone, params, groupBy);
        res.send(analytics);
    } catch (error: any) {
        logger.error(error);
        res.status(400).send({ error: error?.message || error });
    }
}

// GET '/analytics/wa'
const getWaAnalytics = async (req: Request, res: Response) => {
    try {
        const params = { ...req.query, ...req.params } as any;
        let { companyId, timeZone, groupBy, startDate = getDefaultDate().from, endDate = getDefaultDate().to } = params;
        const fromDate = formatDate(startDate);
        const toDate = formatDate(endDate);

        const analytics = await waAnalyticsService.getAnalytics(companyId, fromDate, toDate, timeZone, params, groupBy);
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
        let { companyId, smsNodeIds, waNodeIds, emailNodeIds, timeZone, groupBy, mailGroupBy, startDate = getDefaultDate().from, endDate = getDefaultDate().to } = params;
        const fromDate = formatDate(startDate);
        const toDate = formatDate(endDate);
        if (!companyId) throw "companyId required";
        if (!smsNodeIds?.length && !waNodeIds?.length && !emailNodeIds?.length) throw "smsNodeIds OR waNodeIds OR emailNodeIds required";

        let smsAnalytics, waAnalytics, mailAnalytics;
        if (smsNodeIds?.length) smsAnalytics = await smsAnalyticsService.getAnalytics(companyId, fromDate, toDate, timeZone, params, groupBy);
        if (waNodeIds?.length) waAnalytics = await waAnalyticsService.getAnalytics(companyId, fromDate, toDate, timeZone, params, groupBy);
        if (emailNodeIds?.length) mailAnalytics = await mailAnalyticsService.getAnalytics(companyId, fromDate, toDate, timeZone, params, mailGroupBy);
        res.send({ sms: smsAnalytics, mail: mailAnalytics, wa: waAnalytics });
    } catch (error: any) {
        logger.error(error);
        res.status(400).send({ error: error?.message || error });
    }
}

export {
    getSmsAnalytics,
    getMailAnalytics,
    getWaAnalytics,
    getOtpAnalytics,
    getCampaignAnalytics,
};