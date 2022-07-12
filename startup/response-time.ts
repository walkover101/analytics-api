import { Application, Request, Response } from "express";
import logger from "../logger/logger";
import responseTime from 'response-time';

export default function (app: Application) {
  app.use(
    responseTime(function (req: Request, _res: Response, time: any) {
      let stat = (req.method + req.url)
        .toLowerCase()
        .replace(/[:.]/g, "")
        .replace(/\//g, "_");

      logger.info(`${stat} ${time}`);
    })
  );
};
