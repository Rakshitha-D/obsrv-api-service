import { Request, Response } from "express";
import * as _ from "lodash";
import logger from "../../logger";
import { ResponseHandler } from "../../helpers/ResponseHandler";
import { schemaValidation } from "../../services/ValidationService";
import validationSchema from "./ListTemplateValidationSchema.json";
import { QueryTemplate } from "../../models/QueryTemplate";
import { obsrvError } from "../../types/ObsrvError";
const apiId = "api.query.template.list";

const validateRequest = (req: Request) => {
    const isValidSchema = schemaValidation(req.body, validationSchema);
    if (!isValidSchema?.isValid) {
        throw obsrvError("", "QUERY_TEMPLATE_INVALID_INPUT", isValidSchema?.message, "BAD_REQUEST", 400)
    }
}

export const listQueryTemplates = async (req: Request, res: Response) => {

    validateRequest(req);
    const requestBody = req.body;    
    let templateData = await getTemplateList(requestBody.request);
    templateData = _.map(templateData, (data: any) => {
        return data?.dataValues
    })
    logger.info({ apiId, requestBody, message: `Templates are listed successfully` })
    return ResponseHandler.successResponse(req, res, { status: 200, data: templateData });

}

const getTemplateList = async (req: Record<string, any>) => {
    const limit: any = _.get(req, "limit");
    const offset: any = _.get(req, "offset");
    const order: any = _.get(req, "order");
    const { filters = {} } = req || {};
    const templates = await QueryTemplate.findAll({ limit: limit || 100, offset: offset || 0, order, ...(filters && { where: filters }) })
    return templates
}