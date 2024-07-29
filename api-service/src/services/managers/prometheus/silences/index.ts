import CONSTANTS from '../../constants';

const createSilence = async (payload: Record<string, any>) => {
    throw new Error(CONSTANTS.METHOD_NOT_IMPLEMENTED);
}

const getSilenceMetadata = async (payload: Record<string, any>) => {
    throw new Error(CONSTANTS.METHOD_NOT_IMPLEMENTED)
}

const updateSilence = async (silence: Record<string, any>, payload: Record<string, any>) => {
    throw new Error(CONSTANTS.METHOD_NOT_IMPLEMENTED);
}

const deleteSilence = async (payload: Record<string, any>) => {
    throw new Error(CONSTANTS.METHOD_NOT_IMPLEMENTED);
}

export {
    createSilence,
    getSilenceMetadata,
    updateSilence,
    deleteSilence
}