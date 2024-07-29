export class ObsrvError {
    code: string;
    message: string;
    errCode: string;
    statusCode: number;
    data: any | undefined;
    datasetId: string;
    err: Error | undefined;

    constructor(datasetId: string, code: string, message: string, errorCode: string, statusCode: number, err?: Error, data?: any) {
        this.datasetId = datasetId;
        this.code = code;
        this.message = message;
        this.errCode = errorCode;
        this.statusCode = statusCode;
        this.data = data;
        this.err = err;
    }

}

export const obsrvError = (datasetId: string, code: string, message: string, errorCode: string, statusCode: number, err?: Error, data?: any) => {
    return new ObsrvError(datasetId, code, message, errorCode, statusCode, err, data)
}