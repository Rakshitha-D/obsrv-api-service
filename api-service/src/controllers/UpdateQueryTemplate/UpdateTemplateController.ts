import { Request, Response } from "express";
import * as _ from "lodash"
import { schemaValidation } from "../../services/ValidationService";
import validationSchema from "./UpdateTemplateValidationSchema.json";
import { ResponseHandler } from "../../helpers/ResponseHandler";
import { getQueryTemplate } from "../../services/QueryTemplateService";
import { QueryTemplate } from "../../models/QueryTemplate";
import { validateTemplate } from "../CreateQueryTemplate/QueryTemplateValidator";
import { config } from "../../configs/Config";
import logger from "../../logger";
import { obsrvError } from "../../types/ObsrvError";
const apiId = "api.query.template.update";
const requiredVariables = _.get(config, "template_config.template_required_variables");

const validateRequest = (req: Request) => {
    const isValidSchema = schemaValidation(req.body, validationSchema);
    if (!isValidSchema?.isValid) {
        if (_.includes(isValidSchema.message, "template_name")) {
            _.set(isValidSchema, "message", "Template name should contain alphanumeric characters and single space between characters")
        }
        throw obsrvError("", "QUERY_TEMPLATE_INVALID_INPUT", isValidSchema?.message, "BAD_REQUEST", 400)
    }
}

const validateTemplateExists = async (req: Request, templateId: string) => {
    const isTemplateExists = await getQueryTemplate(templateId)
    if (isTemplateExists === null) {
        throw obsrvError("", "QUERY_TEMPLATE_NOT_EXISTS", `Template ${templateId} does not exists`, "NOT_FOUND", 404)
    }

    const { validTemplate } = await validateTemplate(req.body);
    if (!validTemplate) {
        throw obsrvError("", "QUERY_TEMPLATE_INVALID_INPUT", `Invalid template provided, A template should consist of variables ${requiredVariables} and type of json,sql`, "BAD_REQUEST", 400)
    }
}

export const updateQueryTemplate = async (req: Request, res: Response) => {

    validateRequest(req)
    const requestBody = req.body;
    const templateId = _.get(req, "params.templateId");
    await validateTemplateExists(req, templateId)
    await QueryTemplate.update(requestBody?.request, { where: { template_id: templateId } })
    logger.info({ apiId, templateId, requestBody, message: `Query template updated successfully` })
    ResponseHandler.successResponse(req, res, { status: 200, data: { message: "Query template updated successfully", templateId } });

}

