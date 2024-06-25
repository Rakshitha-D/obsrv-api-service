import axios from "axios";
import { config } from "../configs/Config";
import { logger } from "@project-sunbird/logger";
import { health as postgresHealth } from "../connections/databaseConnection";
import { DatasetType, HealthStatus } from "../types/DatasetModels";
import { createClient } from 'redis';
import { isHealthy as isKafkaHealthy } from "../connections/kafkaConnection";
import { druidHttpService, executeNativeQuery } from "../connections/druidConnection";
import _ from "lodash";
import moment from "moment";
import { SystemConfig } from "./SystemConfig";


const dateFormat = 'YYYY-MM-DDT00:00:00+05:30'

const prometheusInstance = axios.create({ baseURL: config?.query_api?.prometheus?.url, headers: { "Content-Type": "application/json" } });
let isRedisDenormHealthy = false;
let isRedisDedupHealthy = false;
const init = async () => {
  createClient({
    url: `redis://${config.redis_config.denorm_redis_host}:${config.redis_config.denorm_redis_port}`
  })
    .on('error', err => {
      logger.error('unable to connect to denorm redis client', err)
      isRedisDenormHealthy = false
    })
    .on("ready", () => {
      isRedisDenormHealthy = true
    })
    .connect();

  createClient({
    url: `redis://${config.redis_config.dedup_redis_host}:${config.redis_config.dedup_redis_port}`
  })
    .on("ready", () => {
      isRedisDedupHealthy = true
    })
    .on('error', err => {
      isRedisDedupHealthy = false
      logger.error('unable to connect to dedup redis client', err)
    })
    .connect();
}

const getDatasetIdForMetrics = (datasetId: string) => {
  datasetId = datasetId.replace(/-/g, '_')
    .replace(/\./g, '_')
    .replace(/\n/g, '')
    .replace(/[\n\r]/g, '')
  return datasetId;
}

const queryMetrics = (params: Record<string, any> | string) => {
  return prometheusInstance.get("/api/v1/query", { params })
}

export const getInfraHealth = async (isMasterDataset: boolean): Promise<{ components: any, status: string }> => {
  const postgres = await getPostgresStatus()
  const druid = await getDruidHealthStatus()
  const flink = await getFlinkHealthStaus()
  let kafka = await getKafkaHealthStatus()
  let redis = HealthStatus.Healthy
  const components = [
    { "type": "postgres", "status": postgres },
    { "type": "kafka", "status": kafka },
    { "type": "druid", "status": druid },
    { "type": "flink", "status": flink }
  ]
  if (isMasterDataset) {
    redis = await getRedisStatus()
    components.push({ "type": "redis", "status": redis })
  }
  const status = [postgres, redis, kafka, druid, flink].includes(HealthStatus.UnHealthy) ? HealthStatus.UnHealthy : HealthStatus.Healthy
  return { components, status };
}

export const getProcessingHealth = async (dataset: any): Promise<{ components: any, status: string }> => {
  const dataset_id = _.get(dataset, "dataset_id")
  const isMasterDataset = _.get(dataset, "type") == DatasetType.MasterDataset;
  const flink = await getFlinkHealthStaus()
  const { count, health } = await getEventsProcessedToday(dataset_id, isMasterDataset)
  const { count: avgCount, health: avgHealth } = await getAvgProcessingSpeedInSec(dataset_id, isMasterDataset)
  const failure = await getValidationFailure(dataset_id, isMasterDataset)
  const dedupFailure = await getDedupFailure(dataset_id)
  const denormFailure = await getDenormFailure(dataset_id)
  const transformFailure = await getTransformFailure(dataset_id)
  const components = [
    {
      "type": "pipeline",
      "status": flink
    },
    {
      "type": "eventsProcessedCount",
      "count": count,
      "status": health
    },
    {
      "type": "avgProcessingSpeedInSec",
      "count": avgCount,
      "status": avgHealth
    },
    {
      "type": "validationFailuresCount",
      "count": failure?.count,
      "status": failure?.health
    },
    {
      "type": "dedupFailuresCount",
      "count": dedupFailure?.count,
      "status": dedupFailure?.health
    },
    {
      "type": "denormFailureCount",
      "count": denormFailure?.count,
      "status": denormFailure?.health
    },
    {
      "type": "transformFailureCount",
      "count": transformFailure?.count,
      "status": transformFailure?.health
    }
  ]


  const defaultThresholds = await SystemConfig.getThresholds()
  const processingDefaultThreshold: any = _.get(defaultThresholds, "processing")
  let status = HealthStatus.Healthy;
  _.forEach(components, (component: any) => {
    const threshold = processingDefaultThreshold[_.get(component, 'type')]
    if ((threshold && threshold < _.get(component, 'count')) || component.status != HealthStatus.Healthy) {
      status = HealthStatus.UnHealthy
      component.status = HealthStatus.UnHealthy
    }
  })
  return { components, status };
}

export const getQueryHealth = async (datasources: any, dataset: any): Promise<{ components: any, status: string }> => {
  logger.debug(JSON.stringify(datasources))
  const components: any = [];
  let status = HealthStatus.Healthy;
  if (!_.isEmpty(datasources)) {
    const druidTasks = await getDruidIndexerStatus(datasources);
    components.push(
      {
        "type": "indexer",
        "status": _.get(druidTasks, "status"),
        "value": _.get(druidTasks, "value")
      }
    )
  } else {
    components.push({
      "type": "indexer",
      "status": HealthStatus.UnHealthy,
      "value": []
    })
    status = HealthStatus.UnHealthy
  }

  const queriesCount = await getQuriesStatus(dataset?.dataset_id)

  components.push({
    "type": "queriesCount",
    "count": queriesCount.count,
    "status": queriesCount.health
  })

  const avgQueryReponseTimeInSec = await getAvgQueryReponseTimeInSec(dataset?.dataset_id)
  components.push({
    "type": "avgQueryReponseTimeInSec",
    "count": avgQueryReponseTimeInSec.count,
    "status": avgQueryReponseTimeInSec.health
  })

  const queriesFailed = await getQueriesFailedCount(dataset?.dataset_id)
  components.push({
    "type": "queriesFailed",
    "count": queriesFailed.count,
    "status": queriesFailed.health
  })

  const defaultThresholds = await SystemConfig.getThresholds()
  const processingDefaultThreshold: any = _.get(defaultThresholds, "query")

  _.forEach(components, (component: any) => {
    const threshold = processingDefaultThreshold[_.get(component, 'type')]
    if ((threshold && threshold < _.get(component, 'count')) || component.status != HealthStatus.Healthy) {
      status = HealthStatus.UnHealthy
      component.status = HealthStatus.UnHealthy
    }
  })

  return { components, status }
}

const getDruidIndexerStatus = async (datasources: any,) => {
  try {
    const results = await Promise.all(_.map(datasources, (datasource) => getDruidDataourceStatus(datasource['datasource'])))
    const values: any = []
    let status = HealthStatus.Healthy
    _.forEach(results, (result: any) => {
      logger.debug({ result })
      const sourceStatus = _.get(result, "payload.state") == "RUNNING" ? HealthStatus.Healthy : HealthStatus.UnHealthy
      logger.debug({ sourceStatus })
      values.push(
        {
          "type": "druid",
          "datasource": _.get(result, "id"),
          "status": sourceStatus,
        }
      )
      if (sourceStatus == HealthStatus.UnHealthy) {
        status = HealthStatus.UnHealthy
      }
    })
    return { value: values, status }
  } catch (error) {
    logger.error(error)
    return { value: [], status: HealthStatus.UnHealthy }
  }


}

const getDruidDataourceStatus = async (datasourceId: string) => {
  logger.debug(datasourceId)
  const { data } = await druidHttpService.get(`/druid/indexer/v1/supervisor/${datasourceId}/status`)
  return data;
}

const getPostgresStatus = async (): Promise<HealthStatus> => {
  try {
    const postgresStatus = await postgresHealth()
    logger.debug(postgresStatus)
  } catch (error) {
    logger.error('errr: ', error)
    return HealthStatus.UnHealthy
  }
  return HealthStatus.Healthy
}

const getRedisStatus = async () => {
  return isRedisDenormHealthy && isRedisDedupHealthy ? HealthStatus.Healthy : HealthStatus.UnHealthy
}

const getKafkaHealthStatus = async () => {
  try {
    const status = await isKafkaHealthy()
    return status ? HealthStatus.Healthy : HealthStatus.UnHealthy
  } catch (error) {
    return HealthStatus.UnHealthy
  }

}

const getFlinkHealthStaus = async () => {
  try {
    const responses = await Promise.all(
      [axios.get(config?.flink_job_configs?.masterdata_processor_job_manager_url as string + "/jobs"),
      axios.get(config?.flink_job_configs?.pipeline_merged_job_manager_url as string + "/jobs")]
    )
    const isHealthy = _.every(responses, (response: any) => {
      const { data = {} } = response;
      return _.get(data, "jobs[0].status") === "RUNNING"
    })
    return isHealthy ? HealthStatus.Healthy : HealthStatus.UnHealthy;
  } catch (error) {
    logger.error("Unable to get flink status", error)
  }
  return HealthStatus.UnHealthy;
}

const getDruidHealthStatus = async () => {
  try {
    const { data = false } = await druidHttpService.get("/status/health")
    return data ? HealthStatus.Healthy : HealthStatus.UnHealthy
  } catch (error) {
    logger.error("druid health check", error)
    return HealthStatus.UnHealthy
  }
}

const getEventsProcessedToday = async (datasetId: string, isMasterDataset: boolean) => {
  const startDate = moment().format(dateFormat);
  const endDate = moment().add(1, 'd').format(dateFormat);
  const intervals = `${startDate}/${endDate}`
  logger.debug({ datasetId, isMasterDataset })
  try {
    const { data } = await executeNativeQuery({
      "queryType": "timeseries",
      "dataSource": "system-events",
      "intervals": intervals,
      "granularity": {
        "type": "all",
        "timeZone": "Asia/Kolkata"
      },
      "filter": {
        "type": "and",
        "fields": [
          {
            "type": "selector",
            "dimension": "ctx_module",
            "value": "processing"
          },
          {
            "type": "selector",
            "dimension": "ctx_dataset",
            "value": datasetId
          },
          {
            "type": "selector",
            "dimension": "ctx_pdata_id",
            "value": isMasterDataset ? "MasterDataProcessorJob" : "DruidRouterJob"
          },
          {
            "type": "selector",
            "dimension": "error_code",
            "value": null
          }
        ]
      },
      "aggregations": [
        {
          "type": "longSum",
          "name": "count",
          "fieldName": "count"
        }
      ]
    })
    return { health: HealthStatus.Healthy, count: _.get(data, "[0].result.count", 0) || 0 }
  } catch (error) {
    logger.error(error)
    return { count: 0, health: HealthStatus.UnHealthy }
  }
}

const getAvgProcessingSpeedInSec = async (datasetId: string, isMasterDataset: boolean) => {
  const startDate = moment().format(dateFormat);
  const endDate = moment().add(1, 'd').format(dateFormat);
  const intervals = `${startDate}/${endDate}`
  logger.debug({ datasetId, isMasterDataset })
  try {
    const { data } = await executeNativeQuery({
      "queryType": "groupBy",
      "dataSource": "system-events",
      "intervals": intervals,
      "granularity": {
        "type": "all",
        "timeZone": "Asia/Kolkata"
      },
      "filter": {
        "type": "and",
        "fields": [
          {
            "type": "selector",
            "dimension": "ctx_module",
            "value": "processing"
          },
          {
            "type": "selector",
            "dimension": "ctx_dataset",
            "value": datasetId
          },
          {
            "type": "selector",
            "dimension": "ctx_pdata_id",
            "value": isMasterDataset ? "MasterDataProcessorJob" : "DruidRouterJob"
          },
          {
            "type": "selector",
            "dimension": "error_code",
            "value": null
          }
        ]
      },
      "aggregations": [
        {
          "type": "longSum",
          "name": "processing_time",
          "fieldName": "total_processing_time"
        },
        {
          "type": "longSum",
          "name": "count",
          "fieldName": "count"
        }
      ],
      "postAggregations": [
        {
          "type": "expression",
          "name": "average_processing_time",
          "expression": "case_searched((count > 0),(processing_time/count),0",
        }
      ]
    })
    const count = _.get(data, "[0].event.average_processing_time", 0) || 0
    return { health: HealthStatus.Healthy, count: count / 1000 }
  } catch (error) {
    logger.error(error)
    return { count: 0, health: HealthStatus.UnHealthy }
  }
}

const getValidationFailure = async (datasetId: string, isMasterDataset: boolean) => {
  let query = ""
  if (isMasterDataset) {
    query = `sum(sum_over_time(flink_taskmanager_job_task_operator_PipelinePreprocessorJob_${getDatasetIdForMetrics(datasetId)}_validator_failed_count[1d]))`
  }
  else {
    query = `sum(sum_over_time(flink_taskmanager_job_task_operator_PipelinePreprocessorJob_${getDatasetIdForMetrics(datasetId)}_validator_failed_count[1d]))`
  }
  try {
    const { data } = await queryMetrics({ query })
    return { count: _.toInteger(_.get(data, "data.result[0].value[1]", "0")) || 0, health: HealthStatus.Healthy }
  } catch (error) {
    logger.error(error)
    return { count: 0, health: HealthStatus.UnHealthy }
  }
}

const getDedupFailure = async (datasetId: string) => {
  let query = `sum(sum_over_time(flink_taskmanager_job_task_operator_PipelinePreprocessorJob_${getDatasetIdForMetrics(datasetId)}_dedup_failed_count[1d]))`;
  try {
    const { data } = await queryMetrics({ query })
    return { count: _.toInteger(_.get(data, "data.result[0].value[1]", "0")) || 0, health: HealthStatus.Healthy }
  } catch (error) {
    logger.error(error)
    return { count: 0, health: HealthStatus.UnHealthy }
  }
}

const getDenormFailure = async (datasetId: string) => {
  let query = `sum(sum_over_time(flink_taskmanager_job_task_operator_DenormalizerJob_${getDatasetIdForMetrics(datasetId)}_denorm_failed[1d]))`;
  try {
    const { data } = await queryMetrics({ query })
    return { count: _.toInteger(_.get(data, "data.result[0].value[1]", "0")) || 0, health: HealthStatus.Healthy }
  } catch (error) {
    logger.error(error)
    return { count: 0, health: HealthStatus.UnHealthy }
  }
}

const getTransformFailure = async (datasetId: string) => {
  let query = `sum(sum_over_time(flink_taskmanager_job_task_operator_TransformerJob_${getDatasetIdForMetrics(datasetId)}_transform_failed_count[1d]))`;
  try {
    const { data } = await queryMetrics({ query })
    return { count: _.toInteger(_.get(data, "data.result[0].value[1]", "0")) || 0, health: HealthStatus.Healthy }
  } catch (error) {
    logger.error(error)
    return { count: 0, health: HealthStatus.UnHealthy }
  }
}

const getQuriesStatus = async (datasetId: string) => {
  let query = `sum(sum_over_time(node_total_api_calls{entity="data-out", dataset_id="${getDatasetIdForMetrics(datasetId)}"}[1d]))`;
  try {
    const { data } = await queryMetrics({ query })
    logger.debug(data)
    return { count: _.toInteger(_.get(data, "data.result[0].value[1]", "0")) || 0, health: HealthStatus.Healthy }
  } catch (error) {
    logger.error(error)
    return { count: 0, health: HealthStatus.UnHealthy }
  }
}

const getAvgQueryReponseTimeInSec = async (datasetId: string) => {
  let query = `avg(avg_over_time(node_query_response_time{entity='data-out', dataset_id="${getDatasetIdForMetrics(datasetId)}"}[1d]))/1000`;
  try {
    const { data } = await queryMetrics({ query })
    logger.debug(data)
    return { count: +(_.get(data, "data.result[0].value[1]", "0")) || 0, health: HealthStatus.Healthy }
  } catch (error) {
    logger.error(error)
    return { count: 0, health: HealthStatus.UnHealthy }
  }
}

const getQueriesFailedCount = async (datasetId: string) => {
  let query = `sum(sum_over_time(node_failed_api_calls{entity='data-out', dataset_id="${getDatasetIdForMetrics(datasetId)}"}[1d]))`;
  try {
    const { data } = await queryMetrics({ query })
    logger.debug(data)
    return { count: _.toInteger(_.get(data, "data.result[0].value[1]", "0")) || 0, health: HealthStatus.Healthy }
  } catch (error) {
    logger.error(error)
    return { count: 0, health: HealthStatus.UnHealthy }
  }
}



init().catch(err => logger.error(err))