import { ErrorRequestHandler } from "express";
import logger from "../logger/logger";

const errorRequestHandler: ErrorRequestHandler = (err, _req, res) => {
  logger.error(err);
  res.status(500).send('Something failed.');
}

export default errorRequestHandler; 