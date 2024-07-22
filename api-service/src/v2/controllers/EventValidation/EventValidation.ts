import { Request, Response } from "express";
import * as _ from "lodash"
import { schemaValidation } from "../../services/ValidationService";
import { ResponseHandler } from "../../helpers/ResponseHandler";
import validationSchema from "./RequestValidationSchema.json"
import logger from "../../logger";
import { datasetService } from "../../services/DatasetService";
import { obsrvError } from "../../types/ObsrvError";
const apiId = "api.schema.validator";

const validateRequest = (req: Request) => {
    const isValidSchema = schemaValidation(req.body, validationSchema);
    if (!isValidSchema?.isValid) {
        throw obsrvError("", "SCHEMA_VALIDATOR_INVALID_INPUT", isValidSchema?.message, "BAD_REQUEST", 400)
    }
}

const validateDataset = async (req: Request) => {
    const datasetId = _.get(req, "body.request.datasetId");
    const isLive = _.get(req, "body.request.isLive");
    let dataset: any;
    let schema: any;

    if (isLive) {
        dataset = await datasetService.getDataset(datasetId, undefined, true);
        schema = _.get(dataset, "data_schema")
    }

    if (!isLive) {
        dataset = await datasetService.getDraftDataset(datasetId);
        schema = _.get(dataset, "data_schema")
    }

    if (dataset === null) {
        throw obsrvError("", "DATASET_NOT_EXISTS", `Dataset ${datasetId} does not exists`, "NOT_FOUND", 404)
    }

    return schema;
}

export const eventValidation = async (req: Request, res: Response) => {

    const requestBody = req.body;
    const event = _.get(req, "body.request.event");
    validateRequest(req);
    const schema: Record<string, any> | any = await validateDataset(req)
    const validateEventAgainstSchema = schemaValidation(event, _.omit(schema, "$schema"));
    logger.info({ apiId, requestBody, message: validateEventAgainstSchema?.message })
    ResponseHandler.successResponse(req, res, { status: 200, data: { message: validateEventAgainstSchema?.message, isValid: validateEventAgainstSchema?.isValid } });

}

