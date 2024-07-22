import { Request, Response } from "express";
import * as _ from "lodash";
import { deleteTemplate } from "../../services/QueryTemplateService";
import logger from "../../logger";
import { ResponseHandler } from "../../helpers/ResponseHandler";
import { obsrvError } from "../../types/ObsrvError";
const apiId = "api.query.template.delete";

const validateTemplate = async (template_id: string) => {
    const deleteResponse = await deleteTemplate(template_id);
    if (deleteResponse === 0) {
        throw obsrvError("", "QUERY_TEMPLATE_NOT_EXISTS", `Template ${template_id} does not exists`, "NOT_FOUND", 404)
    }
}

export const deleteQueryTemplate = async (req: Request, res: Response) => {

    const template_id = _.get(req, "params.templateId");
    await validateTemplate(template_id);
    logger.info({ apiId, template_id, message: `Templates ${template_id} deleted successfully` })
    ResponseHandler.successResponse(req, res, { status: 200, data: { message: `Template ${template_id} deleted successfully` } });

}