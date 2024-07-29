import express from "express";
import { metricsScrapeHandler } from "../metrics/prometheus";

export const metricRouter = express.Router();
//Scrape metrics to prometheus
metricRouter.get("/metrics", metricsScrapeHandler)