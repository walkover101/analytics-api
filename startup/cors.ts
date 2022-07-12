import { Application } from "express";
import cors from "cors";

export default function (app: Application) {
  app.use(
    cors({
      origin: "*",
      maxAge: 86400,
      preflightContinue: true,
    })
  );
};
