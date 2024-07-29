import axios from "axios";
import { config } from "../configs/Config";

const grafanaHttpClient = axios.create({
  baseURL: config.grafana_config.url
});

grafanaHttpClient.defaults.headers.common['Authorization'] = config.grafana_config.access_token;

export { grafanaHttpClient };