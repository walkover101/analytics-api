import { RowBatch } from '@google-cloud/bigquery';
import express, { Request, Response } from 'express';
import bigquery from '../startup/big-query';
import { getDefaultDate } from '../utility';
const router = express.Router();
const reportQueryMap = new Map();
const requestQueryMap = new Map();
const PROJECT_ID = process.env.GCP_PROJECT_ID;
const DATA_SET = process.env.MSG91_DATASET_ID;
const REQUEST_TABLE = process.env.REQUEST_DATA_TABLE_ID;
const REPORT_TABLE = process.env.REPORT_DATA_TABLE_ID;
router.route('/users/:userId')
    .get(async (req: Request, res: Response) => {
        let { userId = 100079, startDate = getDefaultDate().end, endDate = getDefaultDate().start, interval = INTERVAL.DAILY } = { ...req.query, ...req.params } as any;
        if (!isValidInterval(interval)) {
            res.status(400).send({
                message: "Invalid interval provided",
                validIntervals: Object.values(INTERVAL)
            });
            return;
        }
        console.log(startDate);
        console.log(endDate);
        // const query = `SELECT DATE(sentTime) as Date, EXTRACT(HOUR FROM sentTime) as Hour,
        // user_pid as Company, senderID as ID, SUM(credit) as Credit, 
        // COUNTIF(status = 1) as Delivered, COUNTIF(status = 2) as Failed,
        // COUNTIF(status = 1) + COUNTIF(status= 2) as Sent
        // FROM \`msg91-reports.msg91_production.report_data\`
        // WHERE (sentTime BETWEEN "${endDate}" AND "${startDate}") AND
        // user_pid = "${userId}"
        // GROUP BY DATE(sentTime), EXTRACT(HOUR FROM sentTime), user_pid, senderID;`
        let reportDataQuery = null;
        let requestDataQuery = null;
        try {
            interval = interval.replace(/['"]+/g, '') as any;
            reportDataQuery = getQuery(reportQueryMap.get(interval), { startDate, endDate, userId });
            requestDataQuery = getQuery(requestQueryMap.get(interval), { startDate, endDate, userId });
        } catch (error: any) {
            console.log(error);
            res.send(error && error.message);
            return [];
        }

        // const [reportDataJob] = await bigquery.createQueryJob({
        //     query: reportDataQuery,
        //     location: process.env.DATA_SET_LOCATION,
        //     // maximumBytesBilled: "1000"
        // });
        // const [requestDataJob] = await bigquery.createQueryJob({
        //     query: requestDataQuery,
        //     location: process.env.DATA_SET_LOCATION,
        //     // maximumBytesBilled: "1000"
        // });
        const [[reportDataJob], [requestDataJob]] = await Promise.all([
            bigquery.createQueryJob({
                query: reportDataQuery,
                location: process.env.DATA_SET_LOCATION,
                // maximumBytesBilled: "1000"
            }),
            bigquery.createQueryJob({
                query: requestDataQuery,
                location: process.env.DATA_SET_LOCATION,
                // maximumBytesBilled: "1000"
            })
        ]).catch(reason => {
            console.error(reason);
            return [[], []];
        });


        console.log(getDefaultDate());
        const [[reportRows], [requestRows]] = await Promise.all([reportDataJob.getQueryResults(), requestDataJob.getQueryResults()]).catch(reason => {
            console.error(reason)
            return [[], []];
        });
        const rows: any = mergeRows([...reportRows, ...requestRows].map((row: any) => { return { ...row, "Date": row["Date"].value } }), 'Date');
        const total = {
            "Message": 0,
            "Delivered": 0,
            "TotalCredits": 0,
            "Filtered": 0,
            "AvgDeliveryTime": 0
        }
        let totalDeliveryTime = 0;
        const data = rows.map((row: any, index: any) => {
            total["Message"] += row["Sent"];
            total["Delivered"] += row["Delivered"];
            total["TotalCredits"] += row["BalanceDeducted"];
            total["Filtered"] = total["Message"] - total["Delivered"];
            totalDeliveryTime += row["DeliveryTime"] || 0;
            total["AvgDeliveryTime"] = totalDeliveryTime / (index + 1);
            return row;
        })
        res.send({
            data,
            total
        });
    });

router.route('/users/:userId/campaigns/:campaignId')
    .get((req: Request, res: Response) => {
        const { userId, campaignId, startDate = getDefaultDate().start, endDate = getDefaultDate().end } = req.params;
        res.send("Work in progress");
    });
router.route('/vendors')
    .get(async (req: Request, res: Response) => {
        let { id, startDate = getDefaultDate().end, endDate = getDefaultDate().start, interval = INTERVAL.DAILY } = { ...req.query, ...req.params } as any;
        const reportQuery = `SELECT DATE(sentTime) as Date, smsc, COUNT(_id) as Total,
        SUM(credit) as BalanceDeducted, 
        COUNTIF(status = 1) as Delivered, 
        COUNTIF(status = 2) as Failed,
        COUNTIF(status = 1) + COUNTIF(status= 2) as Sent, 
        COUNTIF(status = 9) as NDNC, 
        COUNTIF(status = 17) as Blocked, 
        COUNTIF(status = 7) as AutoFailed,
        COUNTIF(status = 25) as Rejected,
        ROUND(SUM(IF(status = 1,TIMESTAMP_DIFF(deliveryTime, sentTime, SECOND),NULL))/COUNTIF(status = 1),0) as DeliveryTime
        FROM \`${PROJECT_ID}.${DATA_SET}.${REPORT_TABLE}\`
        WHERE (sentTime BETWEEN "${startDate}" AND "${endDate}")
        GROUP BY DATE(sentTime), smsc;`
        const requestQuery = `SELECT DATE(requestDate) as Date, smsc, COUNT(_id) as Total,
        SUM(credit) as BalanceDeducted, 
        COUNTIF(reportStatus = 1) as Delivered, 
        COUNTIF(reportStatus = 2) as Failed,
        COUNTIF(reportStatus = 1) + COUNTIF(reportStatus= 2) as Sent, 
        COUNTIF(reportStatus = 9) as NDNC, 
        COUNTIF(reportStatus = 17) as Blocked, 
        COUNTIF(reportStatus = 7) as AutoFailed,
        COUNTIF(reportStatus = 25) as Rejected,
        ROUND(SUM(IF(reportStatus = 1,TIMESTAMP_DIFF(deliveryTime, requestDate, SECOND),NULL))/COUNTIF(reportStatus = 1),0) as DeliveryTime
        FROM \`${PROJECT_ID}.${DATA_SET}.${REQUEST_TABLE}\`
        WHERE (requestDate BETWEEN "${startDate}" AND "${endDate}") AND isSingleRequest = "1"
        GROUP BY DATE(requestDate), smsc;`
        // return;
        const [[reportDataJob], [requestDataJob]] = await Promise.all([
            bigquery.createQueryJob({
                query: reportQuery,
                location: process.env.DATA_SET_LOCATION,
                // maximumBytesBilled: "1000"
            }),
            bigquery.createQueryJob({
                query: requestQuery,
                location: process.env.DATA_SET_LOCATION,
                // maximumBytesBilled: "1000"
            })
        ]).catch(reason => {
            console.error(reason);
            return [[], []];
        });
        const [[reportRows], [requestRows]] = await Promise.all([reportDataJob.getQueryResults(), requestDataJob.getQueryResults()]).catch(reason => {
            console.error(reason)
            return [[], []];
        });
        const rows: any = mergeRows([...reportRows, ...requestRows].map((row: any) => { return { ...row, "Date": row["Date"].value, "mergeKey": `${row["Date"].value}-${row["smsc"]}` } }), 'mergeKey');
        res.send(rows.map((row: any) => {
            delete row["mergeKey"];
            return row;
        }));
        return;
    })
router.route('/profit')
    .get(async (req: Request, res: Response) => {
        let { id, startDate = getDefaultDate().end, endDate = getDefaultDate().start, interval = INTERVAL.DAILY } = { ...req.query, ...req.params } as any;
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
        res.send(rows.map(row => {
            return { ...row, "Date": row["Date"].value }
        }))
        return;
    });
router.route('/profit/users/:userId')
    .get(async (req: Request, res: Response) => {
        let { userId, startDate = getDefaultDate().end, endDate = getDefaultDate().start, interval = INTERVAL.DAILY } = { ...req.query, ...req.params } as any;
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
        res.send(rows.map(row => {
            return { ...row, "Date": row["Date"].value }
        }))
        return;
    });
router.route('/profit/vendors')
    .get(async (req: Request, res: Response) => {
        let { userId, startDate = getDefaultDate().end, endDate = getDefaultDate().start, interval = INTERVAL.DAILY } = { ...req.query, ...req.params } as any;
        const query = `SELECT DATE(sentTime) as Date, smsc as Vendor,
        crcy as Currency,
        SUM(credit) as Credit,
        SUM(oppri) as Cost,
        (SUM(credit) - SUM(oppri)) as Profit
        FROM \`${PROJECT_ID}.${DATA_SET}.${REPORT_TABLE}\`
        WHERE (sentTime BETWEEN "${startDate}" AND "${endDate}") AND user_pid = "${userId}"
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
function isValidInterval(interval: string) {
    return Object.values(INTERVAL).some(value => value == interval.replace(/['"]+/g, ''));
}
function getQuery(sqlQuery: string, options: any) {
    sqlQuery = sqlQuery.replace(/{\w+}/g, (placeholder: any) => {
        const key = placeholder.substring(1, placeholder.length - 1);
        const value = options[key];
        if (value) {
            return value;
        }
        throw new Error(`${key} is not provided`)
    });


    return sqlQuery;
}
function mergeRows(rows: any[], mergeKey: string) {
    const map = new Map();
    rows.forEach(row => {
        let key = row[mergeKey];
        if (map.has(key)) {
            map.set(key, mergeObject(row, map.get(key)));
        } else {
            map.set(key, row);
        }
    })
    return Array.from(map.values());
}
function mergeObject(one: any, two: any) {
    Object.keys(one).forEach(currKey => {

        if (currKey == "DeliveryTime") {
            if (one[currKey] && two[currKey]) {
                one[currKey] = ((one[currKey] + two[currKey]) / 2);
            } else {
                one[currKey] ||= two[currKey];
            }
            delete two[currKey];
            return;
        };
        let value = one[currKey];
        switch (typeof value) {
            case 'number':
                one[currKey] += two[currKey] || 0;
                delete two[currKey];
                break;
            case 'string':
                one[currKey] = two[currKey] || one[currKey];
                delete two[currKey];
                break;
            case 'boolean':
                one[currKey] = one[currKey] && two[currKey];
                delete two[currKey];
                break;
            default:
                one = { ...one, ...two };
                break;
        }
    })
    return one;
}
enum INTERVAL {
    // HOURLY = "hourly",
    DAILY = "daily",
    // WEEKLY = "weekly",
    // MONTHLY = "monthly"
}
// reportQueryMap.set(INTERVAL.HOURLY, `SELECT COUNT(_id) as Sent, DATE(sentTime) as Date, EXTRACT(HOUR FROM sentTime) as Hour,
// user_pid as Company,
// SUM(credit) as BalanceDeducted, 
// COUNTIF(status = 1) as Delivered, 
// COUNTIF(status = 2) as Failed,
// COUNTIF(status = 1) + COUNTIF(status= 2) as Sent, 
// COUNTIF(status = 9) as NDNC, 
// COUNTIF(status = 25) as Rejected,
// COUNTIF(status = 17) as Blocked, 
// COUNTIF(status = 7) as AutoFailed,
// ROUND(SUM(IF(status = 1,TIMESTAMP_DIFF(deliveryTime, sentTime, SECOND),NULL))/COUNTIF(status = 1),0) as DeliveryTime
// FROM \`msg91-reports.msg91_production.report_data\`
// WHERE (sentTime BETWEEN "{startDate}" AND "{endDate}") AND
// user_pid = "{userId}"
// GROUP BY DATE(sentTime), EXTRACT(HOUR FROM sentTime), user_pid;`)

reportQueryMap.set(INTERVAL.DAILY, `SELECT COUNT(_id) as Sent, DATE(sentTime) as Date,
user_pid as Company, 
SUM(credit) as BalanceDeducted, 
COUNTIF(status = 1) as Delivered, 
COUNTIF(status = 2) as Failed,
-- COUNTIF(status = 1) + COUNTIF(status= 2) as Sent, 
COUNTIF(status = 9) as NDNC, 
COUNTIF(status = 17) as Blocked, 
COUNTIF(status = 7) as AutoFailed,
COUNTIF(status = 25) as Rejected,
ROUND(SUM(IF(status = 1,TIMESTAMP_DIFF(deliveryTime, sentTime, SECOND),NULL))/COUNTIF(status = 1),0) as DeliveryTime
FROM \`${PROJECT_ID}.${DATA_SET}.${REPORT_TABLE}\`
WHERE (sentTime BETWEEN "{startDate}" AND "{endDate}") AND
user_pid = "{userId}"
GROUP BY DATE(sentTime), user_pid;`);

requestQueryMap.set(INTERVAL.DAILY, `SELECT COUNT(_id) as Sent, DATE(requestDate) as Date,
user_pid as Company, 
SUM(credit) as BalanceDeducted, 
COUNTIF(reportStatus = 1) as Delivered, 
COUNTIF(reportStatus = 2) as Failed,
-- COUNTIF(reportStatus = 1) + COUNTIF(status= '2') as Sent, 
COUNTIF(reportStatus = 9) as NDNC, 
COUNTIF(reportStatus = 17) as Blocked, 
COUNTIF(reportStatus = 7) as AutoFailed,
COUNTIF(reportStatus = 25) as Rejected,
ROUND(SUM(IF(reportStatus = 1,TIMESTAMP_DIFF(deliveryTime, requestDate, SECOND),NULL))/COUNTIF(reportStatus = 1),0) as DeliveryTime
FROM \`${PROJECT_ID}.${DATA_SET}.${REQUEST_TABLE}\`
WHERE (requestDate BETWEEN "{startDate}" AND "{endDate}") AND isSingleRequest = "1" AND
user_pid = "{userId}"
GROUP BY DATE(requestDate), user_pid;`)
export default router;