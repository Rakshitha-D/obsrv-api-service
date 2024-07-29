import { Request, Response } from "express";
import { ResponseHandler } from "../../helpers/ResponseHandler";
import { getDateRange, isValidDateRange } from "../../utils/common";
import { config } from "../../configs/Config";
import moment from "moment";
import { datasetService } from "../../services/DatasetService";
import * as _ from "lodash";
import { cloudProvider } from "../../services/CloudServices";
import { obsrvError } from "../../types/ObsrvError";

const validateDataset = async (type: string, datasetId: string) => {

    if (type && config.cloud_config.exclude_exhaust_types.includes(datasetId)) {
        throw obsrvError("", "RECORD_NOT_FOUND", "Record not found", "NOT_FOUND", 404)
    }

    const datasetRecord = await verifyDatasetExists(datasetId);
    if (datasetRecord === null) {
        throw obsrvError("", "DATASET_NOT_FOUND", "Record not found", "NOT_FOUND", 404)
    }
}

const validateDates = async (type: string, dateRange: Record<string, any>, datasetId: string) => {

    const momentFormat = "YYYY-MM-DD";
    const isValidDates = isValidDateRange(
        moment(dateRange.from, momentFormat),
        moment(dateRange.to, momentFormat),
        config.cloud_config.maxQueryDateRange,
    );
    if (!isValidDates) {
        throw obsrvError("", "INVALID_DATE_RANGE", `Invalid date range! make sure your range cannot be more than ${config.cloud_config.maxQueryDateRange} days`, "BAD_REQUEST", 400)
    }

    const resData: any = await getFromStorage(type, dateRange, datasetId);
    if (_.isEmpty(resData.files)) {
        throw obsrvError("", "NO_BACKUP_FILES_FOUND", "Date range provided does not have any backup files", "NOT_FOUND", 404)
    }

    return resData
}

export const dataExhaust = async (req: Request, res: Response) => {

    const { params } = req;
    const { datasetId } = params;
    const { type }: any = req.query;
    await validateDataset(type, datasetId)
    const dateRange = getDateRange(req);
    const resData: any = await validateDates(type, dateRange, datasetId)
    ResponseHandler.successResponse(req, res, { status: 200, data: resData, })

}

const verifyDatasetExists = async (datasetId: string) => {
    const dataset = await datasetService.getDataset(datasetId, ["id"], true)
    return dataset;
}

const getFromStorage = async (type: string, dateRange: any, datasetId: string) => {
    const resData =
        cloudProvider.getFiles(
            config.cloud_config.container, config.cloud_config.container_prefix, type, dateRange, datasetId,
        )
    return resData || {};
}