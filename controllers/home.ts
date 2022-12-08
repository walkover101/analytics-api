import { Request, Response } from "express";
import { mailReportsConsumer } from "../consumers/mail-reports-consumer";
import logger from '../logger/logger';

// GET '/'
const healthcheck = async (_req: Request, res: Response) => {
    return res.send({ "healthy": true });
}

// POST '/'
const test = async (req: Request, res: Response) => {
    try {
        const result: any = req.body.data;
        const output = await mailReportsConsumer(result);
        
        return res.send(output);
    } catch (err) {
        logger.error(err);
        return res.status(500).send(err);
    }
}

export { healthcheck, test };

function description(description: any): any {
    throw new Error("Function not implemented.");
}
