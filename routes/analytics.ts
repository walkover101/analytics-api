import { RowBatch } from '@google-cloud/bigquery';
import express, { Request, Response } from 'express';
import bigquery from '../services/big-query-service';
import { getDefaultDate } from '../utility';
import logger from "../logger/logger";
const router = express.Router();
const reportQueryMap = new Map();
const requestQueryMap = new Map();
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
        logger.info(startDate);
        logger.info(endDate);
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
            logger.info(error);
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
            logger.error(reason);
            return [[], []];
        });


        logger.info(getDefaultDate());
        const [[reportRows], [requestRows]] = await Promise.all([reportDataJob.getQueryResults(), requestDataJob.getQueryResults()]).catch(reason => {
            logger.error(reason)
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
    logger.info(rows);
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
        logger.info("ONE", one);
        logger.info("TWO", two);
        if (currKey == "DeliveryTime") return;
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
    logger.info("MERGED", one);
    return one;
}
enum INTERVAL {
    HOURLY = "hourly",
    DAILY = "daily",
    // WEEKLY = "weekly",
    // MONTHLY = "monthly"
}
reportQueryMap.set(INTERVAL.HOURLY, `SELECT COUNT(_id) as Sent, DATE(sentTime) as Date, EXTRACT(HOUR FROM sentTime) as Hour,
user_pid as Company,
SUM(credit) as BalanceDeducted, 
COUNTIF(status = 1) as Delivered, 
COUNTIF(status = 2) as Failed,
COUNTIF(status = 1) + COUNTIF(status= 2) as Sent, 
COUNTIF(status = 9) as NDNC, 
COUNTIF(status = 25) as Rejected,
COUNTIF(status = 17) as Blocked, 
COUNTIF(status = 7) as AutoFailed,
ROUND(SUM(IF(status = 1,TIMESTAMP_DIFF(deliveryTime, sentTime, SECOND),NULL))/COUNTIF(status = 1),0) as DeliveryTime
FROM \`msg91-reports.msg91_production.report_data\`
WHERE (sentTime BETWEEN "{startDate}" AND "{endDate}") AND
user_pid = "{userId}"
GROUP BY DATE(sentTime), EXTRACT(HOUR FROM sentTime), user_pid;`)

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
FROM \`msg91-reports.msg91_production.report_data\`
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
FROM \`msg91-reports.msg91_production.request_data\`
WHERE (requestDate BETWEEN "{startDate}" AND "{endDate}") AND isSingleRequest = "1" AND
user_pid = "{userId}"
GROUP BY DATE(requestDate), user_pid;`)
export default router;