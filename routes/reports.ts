import express, { Request, Response } from 'express';
import logger from "../logger/logger";
const router = express.Router();

router.get("/", async (_req: Request, res: Response) => {
    try {
        res.send("Work in progress...");
    } catch (err) {
        logger.error(err);
    }

    res.send({ "healthy": true });
});

export default router;
