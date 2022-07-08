import express, { Request, Response } from "express";
import logger from "../logger/logger";
import { bigQuery } from '../services/big-query-service';
const router = express.Router();

router.get("/", async (_req: Request, res: Response) => {
    try {
        const datasets = await bigQuery.getDatasets();
        if (datasets?.length) return res.send({ "healthy": true });
    } catch (err) {
        logger.error(err);
    }

    res.status(500).send('Couldn\'t load Datasets');
});

export default router;