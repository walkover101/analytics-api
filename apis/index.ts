import express, { Application, Request, Response } from "express";
import { authenticate } from "../middlewares/auth";
import options from "../middlewares/options";
import error from "../middlewares/error";
import responseTime from "response-time";
import homeRoutes from "./home";
import analyticRoutes from "./analytics";
import logRoutes from "./logs";
import downloadRoutes from "./downloads";
import profitRoutes from "./profits";
import dataIntegrity from './data-integrity';
import { generateStatHTML } from "../services/utility-service";
export type Stat = {
  avg: number,
  count: number,
  min: number,
  max: number
}
let statMap: Map<string, Stat> = new Map();
export default function (app: Application) {
  app.use(express.json());
  app.use(express.urlencoded({ extended: true }));
  app.use(responseTime(function (req: Request, res: Response, time) {
    var statId = (req.method + req.originalUrl).toLowerCase()
      .replace(/[:.]/g, '')
      .replace(/\//g, '_')
    updateStat(statId, statMap, time);
  }));
  app.get("/stats", function (req, res) {
    return res.send(generateStatHTML(statMap));
  });
  app.use("/", homeRoutes);
  app.use("/analytics", authenticate, analyticRoutes);
  app.use("/logs", authenticate, logRoutes);
  app.use("/exports", authenticate, downloadRoutes);
  app.use("/profits", authenticate, profitRoutes);
  app.use('/integrity', dataIntegrity);
  app.use(options);
  app.use(error);
}

function updateStat(statId: string, map: Map<string, Stat>, time: number) {
  let oldStat = statMap.get(statId);
  if (oldStat) {
    let newStat = {
      avg: (oldStat.avg + time) / 2,
      count: ++oldStat.count,
      max: Math.max(oldStat.max, time),
      min: Math.min(oldStat.min, time)
    }
    statMap.set(statId, newStat);
  } else {
    statMap.set(statId, {
      avg: time,
      count: 1,
      min: time,
      max: time
    })
  }
}
