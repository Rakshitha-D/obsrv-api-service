import express from "express";
import { config } from "../configs/Config";
import { HTTPConnector } from "../connectors/HttpConnector";
import { PostgresConnector } from "../connectors/PostgresConnector";
import { ResponseHandler } from "../helpers/ResponseHandler";
import { QueryService } from "../services/QueryService";
import { SchemaGeneratorService } from "../services/SchemaGeneratorService";
import { ValidationService } from "../services/ValidationService";
import { DatasetService } from "../services/DatasetService";
import { KafkaConnector } from "../connectors/KafkaConnector";
import { DataSourceService } from "../services/DataSourceService";
import { DbConnector } from "../connectors/DbConnector";
import routes from "./RoutesConfig";

const validationService = new ValidationService();

const queryService = new QueryService(new HTTPConnector(`${config.query_api.druid.host}:${config.query_api.druid.port}`));

const schemaGeneratorService = new SchemaGeneratorService(new PostgresConnector(config.postgres.pg_config));

export const kafkaConnector = new KafkaConnector(config.dataset_api.kafka.config)

export const postgresConnector = new PostgresConnector(config.postgres.pg_config)

export const dbConnector = new DbConnector(config.db_connector_config)

export const dataSourceService = new DataSourceService(dbConnector);

export const datasetService = new DatasetService(dbConnector, kafkaConnector);

const router = express.Router();


/**
 * Query API(s)
 */

router.post(`${routes.QUERY.BASE_PATH}${routes.QUERY.API_VERSION}${routes.QUERY.NATIVE_QUERY.URL}`, ResponseHandler.setApiId(routes.QUERY.NATIVE_QUERY.API_ID), validationService.validateRequestBody, validationService.validateQuery, queryService.executeNativeQuery);
router.post(`${routes.QUERY.BASE_PATH}${routes.QUERY.API_VERSION}${routes.QUERY.SQL_QUERY.URL}`, ResponseHandler.setApiId(routes.QUERY.SQL_QUERY.API_ID), validationService.validateRequestBody, validationService.validateQuery, queryService.executeSqlQuery);


/**
 * Schema Generator API(s)
 */
router.post(`${routes.SCHEMA.BASE_PATH}${routes.SCHEMA.API_VERSION}${routes.SCHEMA.INGESTION_SCHEMA.URL}`, ResponseHandler.setApiId(routes.SCHEMA.INGESTION_SCHEMA.API_ID), validationService.validateRequestBody, schemaGeneratorService.generateIngestionSchema);
router.post(`${routes.SCHEMA.BASE_PATH}${routes.SCHEMA.API_VERSION}${routes.SCHEMA.DATASET_SCHEMA.URL}`, ResponseHandler.setApiId(routes.SCHEMA.DATASET_SCHEMA.API_ID), validationService.validateRequestBody, schemaGeneratorService.generateDataSetSchema);

/**
 * System Configuration labels API
 */

router.post(`${routes.SCHEMA.BASE_PATH}${routes.SCHEMA.API_VERSION}${routes.SYSTEM_SETTINGS.CONFIG_LABEL.URL}`, ResponseHandler.setApiId(routes.SYSTEM_SETTINGS.CONFIG_LABEL.API_ID),);


/***
 * Dataset APIs
 */
router.post(`${routes.DATASET.BASE_PATH}${routes.DATASET.API_VERSION}${routes.DATASET.CREATE.URL}`, ResponseHandler.setApiId(routes.DATASET.CREATE.API_ID), datasetService.create);


/**
 * Config API(s)
 */
router.post(`${routes.CONFIG_OPERATIONS.BASE_PATH}${routes.CONFIG_OPERATIONS.API_VERSION}${routes.CONFIG_OPERATIONS.DATASET.SAVE.URL}`, ResponseHandler.setApiId(routes.CONFIG_OPERATIONS.DATASET.SAVE.API_ID), datasetService.save);
router.patch(`${routes.CONFIG_OPERATIONS.BASE_PATH}${routes.CONFIG_OPERATIONS.API_VERSION}${routes.CONFIG_OPERATIONS.DATASET.SAVE.URL}`, ResponseHandler.setApiId(routes.CONFIG_OPERATIONS.DATASET.SAVE.API_ID), datasetService.update);
router.get(`${routes.CONFIG_OPERATIONS.BASE_PATH}${routes.CONFIG_OPERATIONS.API_VERSION}${routes.CONFIG_OPERATIONS.DATASET.READ.URL}`, ResponseHandler.setApiId(routes.CONFIG_OPERATIONS.DATASET.READ.API_ID), datasetService.read);
router.post(`${routes.CONFIG_OPERATIONS.BASE_PATH}${routes.CONFIG_OPERATIONS.API_VERSION}${routes.CONFIG_OPERATIONS.DATASET.LIST.URL}`, ResponseHandler.setApiId(routes.CONFIG_OPERATIONS.DATASET.LIST.API_ID), datasetService.list);
router.post(`${routes.CONFIG_OPERATIONS.BASE_PATH}${routes.CONFIG_OPERATIONS.API_VERSION}${routes.CONFIG_OPERATIONS.DATASOURCE.SAVE.URL}`, ResponseHandler.setApiId(routes.CONFIG_OPERATIONS.DATASOURCE.SAVE.API_ID), dataSourceService.save);
router.patch(`${routes.CONFIG_OPERATIONS.BASE_PATH}${routes.CONFIG_OPERATIONS.API_VERSION}${routes.CONFIG_OPERATIONS.DATASOURCE.SAVE.URL}`, ResponseHandler.setApiId(routes.CONFIG_OPERATIONS.DATASOURCE.SAVE.API_ID), dataSourceService.update);
router.get(`${routes.CONFIG_OPERATIONS.BASE_PATH}${routes.CONFIG_OPERATIONS.API_VERSION}${routes.CONFIG_OPERATIONS.DATASOURCE.READ.URL}`, ResponseHandler.setApiId(routes.CONFIG_OPERATIONS.DATASOURCE.READ.API_ID), dataSourceService.read);



export { router };
