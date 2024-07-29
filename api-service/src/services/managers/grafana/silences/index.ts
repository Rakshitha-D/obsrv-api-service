import _ from "lodash";
import { addSilence, disableSilence, getCurrentSilenceStatus, transfromPayload } from "./helpers";

const createSilence = async (payload: Record<string, any>) => {
    const { alertId, startDate, endDate, manager } = payload;
    const grafanaPayload = transfromPayload(alertId, startDate, endDate);

    try {   
    const addSilenceResponse = await addSilence(grafanaPayload);
    const silenceId = addSilenceResponse.data.silenceID;

    return {
        silenceId,
        manager,
    }
    
    } catch (err: any) {
        throw new Error(err);
    }
}

const getSilenceMetadata = async (payload: Record<string, any>) => {
    const silenceId = _.get(payload, "id")
    const silenceStatus = await getCurrentSilenceStatus(silenceId);
    return {
        ...payload,
        status: silenceStatus
    }
}

const updateSilence = async (silence: Record<string, any>, payload: Record<string, any>) => {
    const { id, alert_id } = silence;
    const { startTime, endTime } = payload;
    const grafanaPayload = transfromPayload(alert_id, startTime, endTime, id);
    await addSilence(grafanaPayload);
}


const deleteSilence = (silenceId: string) => {
    return disableSilence(silenceId);
}

export { createSilence, getSilenceMetadata, updateSilence, deleteSilence }