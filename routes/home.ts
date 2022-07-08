import express, { Request, Response } from "express";
import logger from "../logger/logger";
import { bigQuery } from '../services/big-query-service';
const router = express.Router();

router.get("/", async (_req: Request, res: Response) => {
    try {
        const datasets = await bigQuery.getDatasets();
        logger.info(datasets);
    } catch (err) {
        logger.error(err);
    }

    res.send({ "healthy": true });
});

export default router;