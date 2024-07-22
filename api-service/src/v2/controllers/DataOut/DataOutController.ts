import { Request, Response } from "express";
import logger from "../../logger";
import { ResponseHandler } from "../../helpers/ResponseHandler";
import { schemaValidation } from "../../services/ValidationService";
import validationSchema from "./DataOutValidationSchema.json";
import { validateQuery } from "./QueryValidator";
import * as _ from "lodash";
import { executeNativeQuery, executeSqlQuery } from "../../connections/druidConnection";
import { obsrvError } from "../../types/ObsrvError";

export const apiId = "api.data.out";

const validateRequest = (req: Request) => {
    const isValidSchema = schemaValidation(req.body, validationSchema);
    if (!isValidSchema?.isValid) {
        throw obsrvError("", "DATA_OUT_INVALID_INPUT", isValidSchema?.message, "BAD_REQUEST", 400)
    }
}

const dataOut = async (req: Request, res: Response) => {

    validateRequest(req)
    const datasetId = req.params?.datasetId;
    const requestBody = req.body;
    
    const isValidQuery: any = await validateQuery(req.body, datasetId);
    const query = _.get(req, "body.query", "")

    if (isValidQuery === true && _.isObject(query)) {
        const result = await executeNativeQuery(query);
        logger.info({ apiId, requestBody, datasetId, message: "Native query executed successfully" })
        return ResponseHandler.successResponse(req, res, {
            status: 200, data: result?.data
        });
    }

    if (isValidQuery === true && _.isString(query)) {
        const result = await executeSqlQuery({ query })
        logger.info({ apiId, requestBody, datasetId, message: "SQL query executed successfully" })
        return ResponseHandler.successResponse(req, res, {
            status: 200, data: result?.data
        });
    }

    else {
        logger.error({ apiId, requestBody, datasetId, message: isValidQuery?.message, code: isValidQuery?.code })
        return ResponseHandler.errorResponse({ message: isValidQuery?.message, statusCode: isValidQuery?.statusCode, errCode: isValidQuery?.errCode, code: isValidQuery?.code }, req, res);
    }
}

export default dataOut;