import express from "express";
import "./startup/dotenv";
import logger from "./logger/logger";
import helmet from './startup/helmet';
import cors from './startup/cors';
import responseTime from './startup/response-time';
import routes from './startup/routes';

const app = express();
helmet(app);
cors(app);
responseTime(app);
routes(app);

const port = process.env.PORT || 3000;
const server = app.listen(port, () =>
    logger.info(`Listening on port ${port}. Environment: ${app.get("env")}...`)
);

module.exports = server;