import { RowBatch } from '@google-cloud/bigquery';
import express, { Request, Response } from 'express';
import bigquery from '../database/big-query-service';
import { getDefaultDate } from '../utility';
import { DateTime } from 'luxon';
import logger from "../logger/logger";
import { runQuery } from './analytics';
const router = express.Router();
const PROJECT_ID = process.env.GCP_PROJECT_ID;
const DATA_SET = process.env.MSG91_DATASET_ID;
const REQUEST_TABLE = process.env.REQUEST_DATA_TABLE_ID;
const REPORT_TABLE = process.env.REPORT_DATA_TABLE_ID;
type options = {
    route?: number,
    timeZone?: string
}
router.route(`/`)
    .post((req: Request, res: Response) => {
        let { companyId, nodeIds, vendorIds, route, startDate = getDefaultDate().end, endDate = getDefaultDate().start } = { ...req.query, ...req.params } as any;
        let body = req.body;
        let smsNodeIds = body?.sms;
        if(!companyId){
            res.status(401).send("comapnyId is required");
        }
        if(smsNodeIds.length <= 0){
            res.status(401).send("nodeIds required");
        }
        return getSMSAnalytics(companyId,smsNodeIds,startDate,endDate);
    })

async function getSMSAnalytics(companyId: string, nodeIds: [string], startDate: DateTime, endDate: DateTime, opt?: options) {
    try {
        startDate = DateTime.fromISO(startDate as any);
        endDate = DateTime.fromISO(endDate as any);
    } catch (error) {
        throw error;
    }
    if (!companyId) {
        throw new Error("companyId is required");
    }
    if (nodeIds?.length <= 0) {
        throw new Error("nodeIds are required");
    }
    // Don't add credit if request gets blocked or NDNC
    const { route, timeZone = "Asia/Kolkata" } = opt || {};
    const query = `SELECT COUNT(report._id) as Sent, DATE(request.requestDate) as Date,
    report.user_pid as Company, 
    ROUND(SUM(IF(report.status = 17 OR report.status = 9,0,report.credit)),2) as BalanceDeducted, 
    COUNTIF(report.status = 1 OR report.status = 3 OR report.status = 26) as Delivered, 
    COUNTIF(report.status = 2 OR report.status = 13 OR report.status = 7) as Failed,
    COUNTIF(report.status = 9) as NDNC, 
    COUNTIF(report.status = 17) as Blocked, 
    COUNTIF(report.status = 7) as AutoFailed,
    COUNTIF(report.status = 25 OR report.status = 16) as Rejected,
    ROUND(SUM(IF(report.status = 1,TIMESTAMP_DIFF(report.deliveryTime, report.sentTime, SECOND),NULL))/COUNTIF(report.status = 1),0) as DeliveryTime FROM \`${PROJECT_ID}.${DATA_SET}.${REPORT_TABLE}\` AS report
    INNER JOIN \`${PROJECT_ID}.${DATA_SET}.${REQUEST_TABLE}\` AS request
    ON report.requestID = request._id
    WHERE (report.sentTime BETWEEN "${startDate.toFormat('yyyy-MM-dd')}" AND "${endDate.plus({ days: 3 }).toFormat('yyyy-MM-dd')}") 
    AND (DATETIME(request.requestDate,"${timeZone}") BETWEEN DATETIME("${startDate.toFormat('yyyy-MM-dd')}","${timeZone}") AND DATETIME("${endDate.toFormat('yyyy-MM-dd')}","${timeZone}"))
    AND report.user_pid = "${companyId}" AND request.requestUserid = "${companyId}"
    AND request.node_id IN (${nodeIds.join()})
    ${route != null ? `AND request.curRoute = "${route}"` : ""}
    GROUP BY DATE(request.requestDate),report.user_pid;`
    console.log(query);
    let result = await runQuery(query);
    result = result.map(row => {
        row['Date'] = row['Date']?.value;
        return row;
    });
    // Sort the result
    result = result.sort((leftRow: any, rightRow: any) => new Date(leftRow['Date']).getTime() - new Date(rightRow['Date']).getTime());
    const total = {
        "Message": 0,
        "Delivered": 0,
        "TotalCredits": 0,
        "Filtered": 0,
        "AvgDeliveryTime": 0
    }
    try {
        let totalDeliveryTime = 0;
        result = result.map((row: any, index: any) => {
            total["Message"] += row["Sent"];
            total["Delivered"] += row["Delivered"];
            total["TotalCredits"] += parseFloat(row["BalanceDeducted"]);
            total["Filtered"] = total["Message"] - total["Delivered"];
            totalDeliveryTime += row["DeliveryTime"] || 0;
            total["AvgDeliveryTime"] = parseFloat(Number(totalDeliveryTime / (index + 1)).toString());
            return row;
        })
        total["TotalCredits"] = Number(parseFloat(total["TotalCredits"].toString()).toFixed(3));
        total["AvgDeliveryTime"] = Number(parseFloat(total["AvgDeliveryTime"].toString()).toFixed(3));
    } catch (error) {
        logger.error(error);
    }

    return { data: result, total };
}

function concat(array:[any]){
    let output = "";
    array.forEach(element=>{
        output += ","+element;
    });
    return output;
}

export default router;