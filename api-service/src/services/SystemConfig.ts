

const defaultThresholds: any = {
    "processing": {
        "avgProcessingSpeedInSec": 300,
        "validationFailuresCount": 5,
        "dedupFailuresCount": 5,
        "denormFailureCount": 5,
        "transformFailureCount": 5
    },
    "query": {
        "avgQueryReponseTimeInSec":  5,
        "queriesFailed": 10
    }
}
export const SystemConfig = {

    getThresholds: (category: string) => {
        return Promise.resolve(defaultThresholds[category])
    }

}