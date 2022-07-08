import express, { Application } from "express";
import home from "../routes/home";
import analytics from "../routes/analytics";
import reports from "../routes/reports";
import options from "../middlewares/options";
import error from "../middlewares/error";

export default function (app: Application) {
  app.use(express.json());
  app.use(express.urlencoded({ extended: true }));
  app.use("/", home);
  app.use("/analytics", analytics);
  app.use("/reports", reports);
  app.use(options);
  app.use(error);
}
