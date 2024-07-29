import express, { Application } from "express";
import {router as v2Router} from "./routes/Router"
import { metricRouter } from "./routes/MetricRouter"
import { druidProxyRouter } from "./routes/DruidProxyRouter"

import bodyParser from "body-parser";
import { errorHandler, obsrvErrorHandler } from "./middlewares/errors";
import { ResponseHandler } from "./helpers/ResponseHandler";
import { config } from "./configs/Config";
import { alertsRouter } from "./routes/AlertsRouter";

const app: Application = express();
 
app.use(bodyParser.json({ limit: config.body_parser_limit}));
app.use(express.text());
app.use(express.json());
app.use(errorHandler)

app.use("/v2/", v2Router);
app.use("/", druidProxyRouter);
app.use("/alerts/v1", alertsRouter);
app.use("/", metricRouter);
app.use("*", ResponseHandler.routeNotFound);
app.use(obsrvErrorHandler);

app.listen(config.api_port, () => {
  console.log(`listening on port ${config.api_port}`);
});

export default app;