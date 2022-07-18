import express, { Request, Response } from 'express';
import logger from "../logger/logger";
const router = express.Router();
import { getDefaultDate } from '../utility';
import bigquery from '../services/big-query-service';
import { INTERVAL } from './analytics';
import { DateTime } from 'luxon';
const PROJECT_ID = process.env.GCP_PROJECT_ID;
const DATA_SET = process.env.MSG91_DATASET_ID;
const REQUEST_TABLE = process.env.REQUEST_DATA_TABLE_ID;
const REPORT_TABLE = process.env.REPORT_DATA_TABLE_ID;
router.route('/')
    .get(async (req: Request, res: Response) => {
        let { companyId, vendorId, startDate = getDefaultDate().end, endDate = getDefaultDate().start, interval = INTERVAL.DAILY } = { ...req.query, ...req.params } as any;
        if (companyId) {
            const result = await getUserProfit(startDate, endDate, companyId);
            return res.send(result);
        } else if (vendorId) {
            const result = await getVendorProfit(startDate, endDate, vendorId);
            return res.send(result);
        } else {
            const result = await getOverallProfit(startDate, endDate);
            return res.send(result);
        }
    });

router.route('/vendors')
    .get(async (req: Request, res: Response) => {
        let { vendorId, startDate = getDefaultDate().end, endDate = getDefaultDate().start, interval = INTERVAL.DAILY } = { ...req.query, ...req.params } as any;
        if (vendorId) {
            const result = await getVendorProfit(startDate, endDate, vendorId);
            return res.send(result);
        }

        const query = `SELECT DATE(sentTime) as Date, smsc as Vendor,
        crcy as Currency,
        SUM(credit) as Credit,
        SUM(oppri) as Cost,
        (SUM(credit) - SUM(oppri)) as Profit
        FROM \`${PROJECT_ID}.${DATA_SET}.${REPORT_TABLE}\`
        WHERE (sentTime BETWEEN "${startDate}" AND "${endDate}")
        GROUP BY DATE(sentTime), smsc, crcy;`
        const [job] = await bigquery.createQueryJob({
            query: query,
            location: process.env.DATA_SET_LOCATION,
            // maximumBytesBilled: "1000"
        });
        const [rows] = await job.getQueryResults();
        res.send(rows.map(row => {
            return { ...row, "Date": row["Date"].value }
        }))
        return;
    });

async function getUserProfit(startDate: DateTime, endDate: DateTime, userId: string) {
    const query = `SELECT DATE(sentTime) as Date, user_pid as Company, SUM(credit) as Credit,
    SUM(oppri) as Cost,
    (SUM(credit) - SUM(oppri)) as Profit
    FROM \`${PROJECT_ID}.${DATA_SET}.${REPORT_TABLE}\`
    WHERE (sentTime BETWEEN "${startDate}" AND "${endDate}") AND user_pid = "${userId}"
    GROUP BY DATE(sentTime), user_pid;`
    const [job] = await bigquery.createQueryJob({
        query: query,
        location: process.env.DATA_SET_LOCATION,
        // maximumBytesBilled: "1000"
    });
    const [rows] = await job.getQueryResults();
    return rows.map(row => {
        return { ...row, "Date": row["Date"].value }
    });
}

async function getVendorProfit(startDate: DateTime, endDate: DateTime, vendorId: string) {
    const query = `SELECT DATE(sentTime) as Date, smsc as Vendor,
    crcy as Currency,
    SUM(credit) as Credit,
    SUM(oppri) as Cost,
    (SUM(credit) - SUM(oppri)) as Profit
    FROM \`${PROJECT_ID}.${DATA_SET}.${REPORT_TABLE}\`
    WHERE (sentTime BETWEEN "${startDate}" AND "${endDate}") AND smsc = "${vendorId}"
    GROUP BY DATE(sentTime), smsc, crcy;`
    const [job] = await bigquery.createQueryJob({
        query: query,
        location: process.env.DATA_SET_LOCATION,
        // maximumBytesBilled: "1000"
    });
    const [rows] = await job.getQueryResults();
    return rows.map(row => {
        return { ...row, "Date": row["Date"].value }
    });
}

async function getOverallProfit(startDate: DateTime, endDate: DateTime) {
    const query = `SELECT DATE(sentTime) as Date, crcy as Currency, SUM(credit) as Credit,
    SUM(oppri) as Cost,
    (SUM(credit) - SUM(oppri)) as Profit
    FROM \`${PROJECT_ID}.${DATA_SET}.${REPORT_TABLE}\`
    WHERE (sentTime BETWEEN "${startDate}" AND "${endDate}")
    GROUP BY DATE(sentTime), crcy;`
    const [job] = await bigquery.createQueryJob({
        query: query,
        location: process.env.DATA_SET_LOCATION,
        // maximumBytesBilled: "1000"
    });
    const [rows] = await job.getQueryResults();
    return rows.map(row => {
        return { ...row, "Date": row["Date"].value }
    });
}
export default router;
