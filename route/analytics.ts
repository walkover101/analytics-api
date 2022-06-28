import express, { Request, Response } from 'express';
import bigquery from '../database/big-query';
import { getDefaultDate } from '../utility';
const router = express.Router();
const queryMap = new Map();

router.route('/users/:userId')
    .get(async (req: Request, res: Response) => {
        const { userId = 100079, startDate = getDefaultDate().end, endDate = getDefaultDate().start, interval = INTERVAL.DAILY } = { ...req.query, ...req.params } as any;
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
        let query = null;
        try {
            query = getQuery(interval, { startDate, endDate, userId });

        } catch (error: any) {
            console.log(error);
            res.send(error && error.message);
            return;
        }

        const [job, stats] = await bigquery.createQueryJob({
            query,
            location: process.env.DATA_SET_LOCATION,
            // maximumBytesBilled: "1000"
        });
        console.log(getDefaultDate());
        const [rows] = await job.getQueryResults().catch(error=>{
            console.log(error);
            return [];
        });
        const total = {
            "Message": 0,
            "Delivered": 0,
            "TotalCredits": 0,
            "Filtered": 0,
            "AvgDeliveryTime": 0
        }
        let totalDeliveryTime = 0;
        const data = rows.map((row, index) => {
            row["Date"] = row["Date"].value;
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
function getQuery(interval: INTERVAL, options: any) {
    interval = interval.replace(/['"]+/g, '') as any;
    var sqlQuery = queryMap.get(interval);

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

enum INTERVAL {
    HOURLY = "hourly",
    DAILY = "daily",
    // WEEKLY = "weekly",
    // MONTHLY = "monthly"
}
queryMap.set(INTERVAL.HOURLY, `SELECT DATE(sentTime) as Date, EXTRACT(HOUR FROM sentTime) as Hour,
user_pid as Company,
SUM(credit) as BalanceDeducted, 
COUNTIF(status = 1) as Delivered, 
COUNTIF(status = 2) as Failed,
COUNTIF(status = 1) + COUNTIF(status= 2) as Sent, 
COUNTIF(status = 9) as NDNC, 
COUNTIF(status = 17) as Blocked, 
COUNTIF(status = 7) as AutoFailed,
ROUND(SUM(IF(status = 1,TIMESTAMP_DIFF(deliveryTime, sentTime, SECOND),NULL))/COUNTIF(status = 1),0) as DeliveryTime
FROM \`msg91-reports.msg91_production.report_data\`
WHERE (sentTime BETWEEN "{startDate}" AND "{endDate}") AND
user_pid = "{userId}"
GROUP BY DATE(sentTime), EXTRACT(HOUR FROM sentTime), user_pid;`)

queryMap.set(INTERVAL.DAILY, `SELECT DATE(sentTime) as Date,
user_pid as Company, 
SUM(credit) as BalanceDeducted, 
COUNTIF(status = 1) as Delivered, 
COUNTIF(status = 2) as Failed,
COUNTIF(status = 1) + COUNTIF(status= 2) as Sent, 
COUNTIF(status = 9) as NDNC, 
COUNTIF(status = 17) as Blocked, 
COUNTIF(status = 7) as AutoFailed,
ROUND(SUM(IF(status = 1,TIMESTAMP_DIFF(deliveryTime, sentTime, SECOND),NULL))/COUNTIF(status = 1),0) as DeliveryTime
FROM \`msg91-reports.msg91_production.report_data\`
WHERE (sentTime BETWEEN "{startDate}" AND "{endDate}") AND
user_pid = "{userId}"
GROUP BY DATE(sentTime), user_pid;`);


export default router;