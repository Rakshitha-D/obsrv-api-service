import { Request, Response } from "express";
import logger from "../../logger";
import { schemaValidation } from "../../services/ValidationService";
import validationSchema from "./CreateTemplateValidationSchema.json";
import { ResponseHandler } from "../../helpers/ResponseHandler";
import { getQueryTemplate } from "../../services/QueryTemplateService";
import * as _ from "lodash";
import { validateTemplate } from "./QueryTemplateValidator";
import { QueryTemplate } from "../../models/QueryTemplate";
import slug from "slug";
import { config } from "../../configs/Config";
import { obsrvError } from "../../types/ObsrvError";
const apiId = "api.query.template.create";
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

const validateTemplateExists = async (req: Request, templateName: string, templateId: string) => {
    const isTemplateExists = await getQueryTemplate(templateId)
    if (isTemplateExists !== null) {
        throw obsrvError("", "QUERY_TEMPLATE_ALREADY_EXISTS", `Template ${templateName} already exists`, "CONFLICT", 409)
    }

    const { validTemplate } = await validateTemplate(req.body);
    if (!validTemplate) {
        throw obsrvError("", "QUERY_TEMPLATE_INVALID_INPUT", `Invalid template provided, A template should consist of variables ${requiredVariables} and type of json,sql`, "BAD_REQUEST", 400)
    }
}

export const createQueryTemplate = async (req: Request, res: Response) => {

    validateRequest(req)
    const templateName = _.get(req, "body.request.template_name");
    const templateId: string = slug(templateName, "_");

    await validateTemplateExists(req, templateName, templateId)
    const data = transformRequest(req.body, templateName);
    await QueryTemplate.create(data)
    logger.info({ apiId, requestBody: req?.body, message: `Query template created successfully` })
    return ResponseHandler.successResponse(req, res, { status: 200, data: { template_id: templateId, template_name: templateName, message: `The query template has been saved successfully` } });

}

const transformRequest = (req: any, templateName: string) => {
    const type: any = _.get(req, "request.query_type");
    const query = _.get(req, "request.query")
    const data = {
        template_id: slug(templateName, "_"),
        template_name: templateName,
        query_type: type,
        query: query
    }
    return data
}
