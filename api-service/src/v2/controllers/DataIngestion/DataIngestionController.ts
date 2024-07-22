import { Request, Response } from "express";
import * as _ from "lodash";
import validationSchema from "./validationSchema.json";
import { schemaValidation } from "../../services/ValidationService";
import { ResponseHandler } from "../../helpers/ResponseHandler";
import { send } from "../../connections/kafkaConnection";
import { datasetService } from "../../services/DatasetService";
import { config } from "../../configs/Config";
import { obsrvError } from "../../types/ObsrvError";

const validateRequest = (req: Request) => {
    const isValidSchema = schemaValidation(req.body, validationSchema)
    if (!isValidSchema?.isValid) {
        throw obsrvError("", "DATA_INGESTION_INVALID_INPUT", isValidSchema?.message, "BAD_REQUEST", 400)
    }
}

const validateDataset = async (datasetId: string) => {

    const dataset = await datasetService.getDataset(datasetId, ["id"], true)
    if (!dataset) {
        throw obsrvError("", "DATASET_NOT_FOUND", `Dataset with id ${datasetId} not found.`, "NOT_FOUND", 404)
    }

    const entryTopic = _.get(dataset, "dataValues.dataset_config.entry_topic")
    if (!entryTopic) {
        throw obsrvError("", "TOPIC_NOT_FOUND", "Entry topic not found", "NOT_FOUND", 404)
    }

    return dataset
}

const dataIn = async (req: Request, res: Response) => {

    validateRequest(req);
    const requestBody = req.body;
    const datasetId = req.params.datasetId.trim();
    const dataset = await validateDataset(datasetId)
    await send(addMetadataToEvents(datasetId, requestBody), _.get(dataset, "dataValues.dataset_config.entry_topic"))
    ResponseHandler.successResponse(req, res, { status: 200, data: { message: "Data ingested successfully" } });

}

const addMetadataToEvents = (datasetId: string, payload: any) => {
    const validData = _.get(payload, "data");
    const now = Date.now();
    const mid = _.get(payload, "params.msgid");
    const source = { id: "api.data.in", version: config?.version, entry_source: "api" };
    const obsrvMeta = { syncts: now, flags: {}, timespans: {}, error: {}, source: source };
    if (Array.isArray(validData)) {
        const payloadRef = validData.map((event: any) => {
            event = _.set(event, "obsrv_meta", obsrvMeta);
            event = _.set(event, "dataset", datasetId);
            event = _.set(event, "msgid", mid);
            return event
        })
        return payloadRef;
    }
    else {
        _.set(validData, "msgid", mid);
        _.set(validData, "obsrv_meta", obsrvMeta);
        _.set(validData, "dataset", datasetId);
        return validData
    }
}

export default dataIn;
