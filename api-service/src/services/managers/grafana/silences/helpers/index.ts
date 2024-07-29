import _ from "lodash";
import { grafanaHttpClient } from "../../../../../connections/grafanaConnection";

const addSilence = (payload: Record<string, any>) => {
    return grafanaHttpClient.post("/api/alertmanager/grafana/api/v2/silences", payload);
}

const fetchAllSilences = () => {
    return grafanaHttpClient.get("/api/alertmanager/grafana/api/v2/silences");
}

const getSilence = (silenceId: string) => {
    return grafanaHttpClient.get(`/api/alertmanager/grafana/api/v2/silence/${silenceId}`);
}

const disableSilence = (silenceId: string) => {
    return grafanaHttpClient.delete(`/api/alertmanager/grafana/api/v2/silence/${silenceId}`);
}

const getCurrentSilenceStatus = async (silenceId: string) => { 
    const response = await getSilence(silenceId);
    const currentSilenceStatus = _.get(response, 'data.status.state');
    return currentSilenceStatus;
}

const transfromPayload = (alertId: string, startDate: string, endDate: string, silenceId?: string,) => {
    return {
        "comment": "Silence for alert: " + alertId,
        "createdby": "admin",
        "startsat": startDate,
        "endsat": endDate,
        "id": silenceId,
        "matchers": [
            {
                name: "alertId",
                value: alertId,
                isRegex: false,
                isEqual: true
            }
        ]
    }
}

export {
    addSilence,
    fetchAllSilences,
    disableSilence,
    getCurrentSilenceStatus,
    transfromPayload
}