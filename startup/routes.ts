import express, { Application } from "express";
import home from "../routes/home";
import analytics from "../routes/analytics";
import downloads from "../routes/downloads";
import profits from '../routes/profits';
import options from "../middlewares/options";
import error from "../middlewares/error";

export default function (app: Application) {
  app.use(express.json());
  app.use(express.urlencoded({ extended: true }));
  app.use("/", home);
  app.use("/analytics", analytics);
  app.use("/exports", downloads);
  app.use("/profits", profits);
  app.use(options);
  app.use(error);
}
