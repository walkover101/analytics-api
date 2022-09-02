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
  app.use((req, res, next) => {
    if (req.method === "OPTIONS") {
      res.setHeader('Cache-Control', 'public, max-age=86400');
      res.end();
    } else {
      next();
    }
  });
};
