import { Request, Response } from "express"
import { ResponseHandler } from "../../helpers/ResponseHandler";
import httpStatus from "http-status";
import _ from "lodash";
import logger from "../../logger";
import { schemaValidation } from "../../services/ValidationService";
import GenerateURL from "./GenerateSignedURLValidationSchema.json"
import { cloudProvider } from "../../services/CloudServices";
import { config } from "../../configs/Config";
import { URLAccess } from "../../types/SampleURLModel";
import { v4 as uuidv4 } from "uuid";
import path from "path";
import { obsrvError } from "../../types/ObsrvError";

export const apiId = "api.files.generate-url"
export const code = "FILES_GENERATE_URL_FAILURE"
const maxFiles = config.presigned_url_configs.maxFiles

const validateRequest = async (req: Request) => {

    const isRequestValid: Record<string, any> = schemaValidation(req.body, GenerateURL)
    if (!isRequestValid.isValid) {
        throw obsrvError("", "GENERATE_SIGNED_URL_INVALID_INPUT", isRequestValid.message, "BAD_REQUEST", 400)
    }

    const { files } = req.body.request;
    const isLimitExceed = _.size(files) > maxFiles
    if (isLimitExceed) {
        throw obsrvError("", "FILES_URL_GENERATION_LIMIT_EXCEED", `Pre-signed URL generation failed: limit of ${maxFiles} exceeded.`, "BAD_REQUEST", 400)
    }

}

const generateSignedURL = async (req: Request, res: Response) => {

    await validateRequest(req)
    const { files, access = URLAccess.Write } = req.body.request;

    const { filesList, updatedFileNames } = transformFileNames(files, access)
    logger.info(`Updated file names with path:${updatedFileNames}`)

    const urlExpiry: number = getURLExpiry(access)
    const preSignedUrls = await Promise.all(cloudProvider.generateSignedURLs(config.cloud_config.container, updatedFileNames, access, urlExpiry))
    const signedUrlList = _.map(preSignedUrls, list => {
        const fileNameWithUid = _.keys(list)[0]
        return {
            filePath: getFilePath(fileNameWithUid),
            fileName: filesList.get(fileNameWithUid),
            preSignedUrl: _.values(list)[0]
        }
    })

    logger.info({ apiId, response: signedUrlList, message: `Sample urls generated successfully for files:${files}` })
    ResponseHandler.successResponse(req, res, { status: httpStatus.OK, data: signedUrlList })
}

const getFilePath = (file: string) => {
    return `${config.cloud_config.container}/${config.presigned_url_configs.service}/user_uploads/${file}`
}

const transformFileNames = (fileList: Array<string | any>, access: string): Record<string, any> => {
    if (access === URLAccess.Read) {
        return transformReadFiles(fileList)
    }
    return transformWriteFiles(fileList)
}

const transformReadFiles = (fileNames: Array<string | any>) => {
    const fileMap = new Map();
    const updatedFileNames = _.map(fileNames, file => {
        fileMap.set(file, file)
        return getFilePath(file)
    })
    return { filesList: fileMap, updatedFileNames }
}

const transformWriteFiles = (fileNames: Array<string | any>) => {
    const fileMap = new Map();
    const updatedFileNames = _.map(fileNames, file => {
        const uuid = uuidv4().replace(/-/g, "").slice(0, 6);
        const ext = path.extname(file)
        const baseName = path.basename(file, ext)
        const updatedFileName = `${baseName}_${uuid}${ext}`
        fileMap.set(updatedFileName, file)
        return getFilePath(updatedFileName)
    })
    return { filesList: fileMap, updatedFileNames }

}

const getURLExpiry = (access: string) => {
    return access === URLAccess.Read ? config.presigned_url_configs.read_storage_url_expiry : config.presigned_url_configs.write_storage_url_expiry
}

export default generateSignedURL;