import express, { Request, Response } from "express";
import logger from "../logger/logger";
import ReportData from '../models/report-data.model';

const router = express.Router();

router.post("/", async (req: Request, res: Response) => {
    try {
        const result: any = [];

        req.body.mobileNumbers?.forEach((mobileNumber: any) => {
            result.push(new ReportData({ telNum: mobileNumber }));
        })

        return res.send(result);
    } catch (err) {
        logger.error(err);
    }

    res.status(500).send('Something went wrong');
});

export default router;