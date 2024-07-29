import { Request, Response } from "express";
import { getQueryTemplate } from "../../services/QueryTemplateService";
import logger from "../../logger";
import { ResponseHandler } from "../../helpers/ResponseHandler";
import * as _ from "lodash";
import { handleTemplateQuery } from "./QueryTemplateHelpers";
import { schemaValidation } from "../../services/ValidationService";
import validationSchema from "./QueryTemplateValidationSchema.json";
import { obsrvError } from "../../types/ObsrvError";
export const apiId = "api.query.template.query";

const validateRequest = (req: Request) => {
    const isValidSchema = schemaValidation(req.body, validationSchema);
    if (!isValidSchema?.isValid) {
        throw obsrvError("", "QUERY_TEMPLATE_INVALID_INPUT", isValidSchema?.message, "BAD_REQUEST", 400)
    }
}

const validateTemplate = async (template_id: string) => {
    const template = await getQueryTemplate(template_id);
    if (template === null) {
        throw obsrvError("", "QUERY_TEMPLATE_NOT_EXISTS", `Template ${template_id} does not exists`, "NOT_FOUND", 404)
    }
    return template
}

export const queryTemplate = async (req: Request, res: Response) => {
    
    validateRequest(req);
    const template_id = _.get(req, "params.templateId");
    const requestBody = _.get(req, "body");

    const template = await validateTemplate(template_id)
    const response = await handleTemplateQuery(req, res, template?.dataValues?.query, template?.dataValues?.query_type)
    logger.info({ apiId, template_id, query: template?.dataValues?.query,query_type: template?.dataValues?.query_type, requestBody, message: `Query executed successfully`})
    return ResponseHandler.successResponse(req, res, {
        status: 200, data: response?.data
    });

}
