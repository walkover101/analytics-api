import express, { Request, Response } from "express";
import logger from "../logger/logger";

const router = express.Router();

router.get("/", async (req: Request, res: Response) => {
    try {
        return res.send({ info: 'test' });
    } catch (err) {
        logger.error(err);
    }

    res.status(500).send('Something went wrong');
});

export default router;