import { Application } from "express";
import helmet from "helmet";

export default function (app: Application) {
  app.use(
    helmet({
      contentSecurityPolicy: false,
    })
  );
};
