import express, { Application } from "express";
import home from "../routes/home";
import dummy from "../routes/dummy";
import analytics from "../routes/analytics";
import downloads from "../routes/downloads";
import campaign from '../routes/campaign';
import profits from '../routes/profits';
import options from "../middlewares/options";
import error from "../middlewares/error";

export default function (app: Application) {
  app.use(express.json());
  app.use(express.urlencoded({ extended: true }));
  app.use("/", home);
  app.use("/dummy", dummy);
  app.use("/analytics", analytics); // Temp for compatibility
  app.use("/analytics/sms", analytics);
  app.use("/exports", downloads);
  app.use("/mail/exports", downloads);
  app.use("/profits/sms", profits);
  app.use("/campaigns",campaign);
  app.use(options);
  app.use(error);
}
