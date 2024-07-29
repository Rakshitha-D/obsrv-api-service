import { Request, Response } from "express";
import * as _ from "lodash";
import { getQueryTemplate } from "../../services/QueryTemplateService";
import logger from "../../logger";
import { ResponseHandler } from "../../helpers/ResponseHandler";
import { obsrvError } from "../../types/ObsrvError";
const apiId = "api.query.template.read";

const validateTemplate = async (req: Request) => {
    const template_id = _.get(req, "params.templateId");
    const template = await getQueryTemplate(template_id);

    if (template === null) {
        throw obsrvError("", "QUERY_TEMPLATE_NOT_EXISTS", `Template ${template_id} does not exists`, "NOT_FOUND", 404)
    }
    return template
}

export const readQueryTemplate = async (req: Request, res: Response) => {

        const template = await validateTemplate(req)
        const template_id = _.get(req, "params.templateId");
        logger.info({ apiId, template_id, message: `Template read successfully with id: ${template_id}`, response: { status: 200, data: template?.dataValues } })
        ResponseHandler.successResponse(req, res, { status: 200, data: template?.dataValues });

}