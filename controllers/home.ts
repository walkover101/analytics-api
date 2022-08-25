import { Request, Response } from "express";
import mongoService from "../database/mongo-service";
import logger from '../logger/logger';
import { MongoClient } from 'mongodb';

const REQUEST_DATA_COLLECTION = process.env.REQUEST_DATA_COLLECTION || '';

// GET '/'
const healthcheck = async (_req: Request, res: Response) => {
    return res.send({ "healthy": true });
}

// POST '/'
const test = async (req: Request, res: Response) => {
    try {
        mongoService().on("connect", (connection: MongoClient) => {
            const collection = connection.db().collection(REQUEST_DATA_COLLECTION);
            collection.insertMany(req.body.data);
        });
        const result: any = req.body.data;

        return res.send(result);
    } catch (err) {
        logger.error(err);
        return res.status(500).send(err);
    }
}

export { healthcheck, test };