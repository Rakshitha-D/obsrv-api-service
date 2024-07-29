import { Request, Response } from "express";
import _ from "lodash";
import { config } from "../../configs/Config";
import logger from "../../logger";
import { ResponseHandler } from "../../helpers/ResponseHandler";
import { druidHttpService } from "../../connections/druidConnection";
import { obsrvError } from "../../types/ObsrvError";

const apiId = "api.obsrv.data.sql-query";

const validateRequest = (req: Request) => {
    if (_.isEmpty(req.body)) {
        throw obsrvError("", "SQL_QUERY_EMPTY_REQUEST", `Failed to query. Invalid request`, "BAD_REQUEST", 400)
    }
}

export const sqlQuery = async (req: Request, res: Response) => {

    validateRequest(req)
    const authorization = _.get(req, ["headers", "authorization"]);
    const result = await druidHttpService.post(`${config.query_api.druid.sql_query_path}`, req.body, {
        headers: { Authorization: authorization },
    });

    logger.info({ message: "Successfully fetched data using sql query", apiId })
    ResponseHandler.flatResponse(req, res, result)

}