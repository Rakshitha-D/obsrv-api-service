import { Request, Response } from "express";
import _ from "lodash";
import { schemaValidation } from "../../services/ValidationService";
import DatasetHealthRequestSchema from "./DatasetHealthValidationSchema.json"
import { ResponseHandler } from "../../helpers/ResponseHandler";
import { DatasetStatus } from "../../types/DatasetModels";
import { getDatasetHealth, getInfraHealth } from "../../services/DatasetHealthService";
import { obsrvError } from "../../types/ObsrvError";
import httpStatus from "http-status";
import { datasetService } from "../../services/DatasetService";
export const apiId = "api.dataset.health";

const validateRequest = async (req: Request) => {
    const isRequestValid: Record<string, any> = schemaValidation(req.body, DatasetHealthRequestSchema)
    if (!isRequestValid.isValid) {
        throw obsrvError("", "DATASET_HEALTH_INVALID_INPUT", isRequestValid.message, "BAD_REQUEST", 400)
    }
}

const datasetHealth = async (req: Request, res: Response) => {
    const dataset_id = _.get(req, ["body", "request", "dataset_id"]);
    const categories = _.get(req, ["body", "request", "categories"])

    validateRequest(req)
    if (dataset_id === "*") {
        const { components, status } = await getInfraHealth(false)
        return ResponseHandler.successResponse(req, res, {
            status: httpStatus.OK, data: {
                status,
                details: [
                    {
                        category: "infra",
                        status,
                        components
                    }]
            }
        });
    }
    const dataset = await datasetService.findDatasets({ id: dataset_id, status: DatasetStatus.Live }, ["dataset_id", "status", "type"])
    if (_.isEmpty(dataset)) {
        throw obsrvError(dataset, "DATASET_NOT_FOUND", `Dataset with the given dataset_id:${dataset} not found`, "NOT_FOUND", 404);
    }
    const datasetHealthData =  await getDatasetHealth(categories, dataset[0])
    return ResponseHandler.successResponse(req, res, {
        status: httpStatus.OK, data: datasetHealthData
    });
}

export default datasetHealth;