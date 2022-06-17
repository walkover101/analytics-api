import express, { Request, Response } from 'express';
import bigquery from '../database/big-query';
import { getDefaultDate } from '../utility';
const router = express.Router();
router.route('/users/:userId')
    .get(async (req: Request, res: Response) => {
        const { userId, startDate = getDefaultDate().start, endDate = getDefaultDate().end } = req.params;
        console.log(startDate);
        console.log(endDate);
        const query = `SELECT DATE(sentTime) as Date, EXTRACT(HOUR FROM sentTime) as Hour,
        user_pid as Company, senderID as ID, SUM(credit) as Credit, 
        COUNTIF(status = 1) as Delivered, COUNTIF(status = 2) as Failed,
        COUNTIF(status = 1) + COUNTIF(status= 2) as Sent
        FROM \`msg91-reports.msg91_production.report_data\`
        WHERE (sentTime BETWEEN "${endDate}" AND "${startDate}") AND
        user_pid = "${userId}"
        GROUP BY DATE(sentTime), EXTRACT(HOUR FROM sentTime), user_pid, senderID;`
        const [job, stats] = await bigquery.createQueryJob({
            query,
            location: process.env.DATA_SET_LOCATION,
            // maximumBytesBilled: "1000"
        });
        console.log(getDefaultDate());
        const [rows] = await job.getQueryResults();
        
        res.send(rows.map(row=>{
            row["Date"] = row["Date"].value;
            return row;
        }));
    });

router.route('/users/:userId/campaigns/:campaignId')
    .get((req: Request, res: Response) => {
        const { userId, campaignId, startDate = getDefaultDate().start, endDate = getDefaultDate().end } = req.params;
        res.send("Work in progress");
    });
export default router;

