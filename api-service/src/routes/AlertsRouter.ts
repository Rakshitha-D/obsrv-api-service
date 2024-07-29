import express from "express";
import notificationHandler from '../controllers/NotificationChannel/Notification';
import { setDataToRequestObject } from "../middlewares/setDataToRequestObject";
import customAlertHandler from "../controllers/Alerts/Alerts";
import metricAliasHandler from "../controllers/Alerts/Metric";
import silenceHandler from "../controllers/Alerts/Silence";

export const alertsRouter = express.Router();

// Notifications

alertsRouter.post("/notifications/search", setDataToRequestObject("api.alert.notification.list"), notificationHandler.listHandler);
alertsRouter.post("/notifications/create", setDataToRequestObject("api.alert.notification.create"), notificationHandler.createHandler);
alertsRouter.get("/notifications/publish/:id", setDataToRequestObject("api.alert.notification.publish"), notificationHandler.publishHandler);
alertsRouter.post("/notifications/test", setDataToRequestObject("api.alert.notification.test"), notificationHandler.testNotifationChannelHandler);
alertsRouter.patch("/notifications/update/:id", setDataToRequestObject("api.alert.notification.update"), notificationHandler.updateHandler);
alertsRouter.delete("/notifications/delete/:id", setDataToRequestObject("api.alert.notification.retire"), notificationHandler.retireHandler);
alertsRouter.get("/notifications/get/:id", setDataToRequestObject("api.alert.notification.get"), notificationHandler.fetchHandler);

// alerts
alertsRouter.post("/create", setDataToRequestObject("api.alert.create"), customAlertHandler.createAlertHandler);
alertsRouter.get("/publish/:alertId", setDataToRequestObject("api.alert.publish"), customAlertHandler.publishAlertHandler);
alertsRouter.post(`/search`, setDataToRequestObject("api.alert.list"), customAlertHandler.searchAlertHandler);
alertsRouter.get("/get/:alertId", setDataToRequestObject("api.alert.getAlertDetails"), customAlertHandler.alertDetailsHandler);
alertsRouter.delete("/delete/:alertId", setDataToRequestObject("api.alert.delete"), customAlertHandler.deleteAlertHandler);
alertsRouter.delete("/delete", setDataToRequestObject("api.alert.delete"), customAlertHandler.deleteSystemAlertsHandler);
alertsRouter.patch("/update/:alertId", setDataToRequestObject("api.alert.update"), customAlertHandler.updateAlertHandler);

// metrics
alertsRouter.post("/metric/alias/create",setDataToRequestObject("api.metric.add"), metricAliasHandler.createMetricHandler);
alertsRouter.post("/metric/alias/search", setDataToRequestObject("api.metric.list"), metricAliasHandler.listMetricsHandler);
alertsRouter.patch("/metric/alias/update/:id", setDataToRequestObject("api.metric.update"),metricAliasHandler.updateMetricHandler);
alertsRouter.delete("/metric/alias/delete/:id", setDataToRequestObject("api.metric.remove"),metricAliasHandler.deleteMetricHandler);
alertsRouter.delete("/metric/alias/delete", setDataToRequestObject("api.metric.remove"), metricAliasHandler.deleteMultipleMetricHandler);

// silence
alertsRouter.post("/silence/create",setDataToRequestObject("api.alert.silence.create"),silenceHandler.createHandler);
alertsRouter.get("/silence/search",setDataToRequestObject("api.alert.silence.list"),silenceHandler.listHandler);
alertsRouter.get("/silence/get/:id",setDataToRequestObject("api.alert.silence.get"),silenceHandler.fetchHandler);
alertsRouter.patch("/silence/update/:id",setDataToRequestObject("api.alert.silence.edit"),silenceHandler.updateHandler);
alertsRouter.delete("/silence/delete/:id",setDataToRequestObject("api.alert.silence.delete"),silenceHandler.deleteHandler);