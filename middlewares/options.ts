import { RequestHandler } from "express";

const requestHandler: RequestHandler = (req, res, next) => {
  if (req.method === "OPTIONS") {
    res.setHeader('Cache-Control', 'public, max-age=86400');
    res.end();
  } else {
    next();
  }
}

export default requestHandler;
