export const TestInputsForDatasetRead = {
    DRAFT_SCHEMA: {
        "dataset_id": "sb-telemetry",
        "id": "sb-telemetry",
        "name": "sb-telemetry",
        "type": "dataset",
        "validation_config": {
            "validate": true,
            "mode": "Strict"
        },
        "extraction_config": {
            "is_batch_event": true,
            "extraction_key": "events",
            "dedup_config": {
                "drop_duplicates": true,
                "dedup_key": "id",
                "dedup_period": 604800
            }
        },
        "dedup_config": {
            "drop_duplicates": true,
            "dedup_key": "mid",
            "dedup_period": 604800
        },
        "data_schema": {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": {
                "eid": {
                    "type": "string"
                },
                "ver": {
                    "type": "string"
                },
                "required": [
                    "eid"
                ]
            },
            "additionalProperties": true
        },
        "router_config": {
            "topic": ""
        },
        "denorm_config": {
            "redis_db_host": "localhost",
            "redis_db_port": 6379,
            "denorm_fields": [
                {
                    "denorm_key": "actor.id",
                    "denorm_out_field": "userdata"
                }
            ]
        },
        "dataset_config": {
            "data_key": "eid",
            "timestamp_key": "ets",
            "entry_topic": "local.ingest",
            "redis_db_host": "localhost",
            "redis_db_port": 6379,
            "index_data": true,
            "redis_db": 0
        },
        "tags": [
            "tag1",
            "tag2"
        ],
        "status": "Draft",
        "version": 1,
        "client_state": {},
        "created_by": "SYSTEM",
        "updated_by": "SYSTEM",
        "created_date": "",
        "update_date": "",
        "published_date": ""
    },
    LIVE_SCHEMA: {

        "dataset_id": "sb-telemetry",
        "id": "sb-telemetry",
        "name": "sb-telemetry",
        "type": "dataset",
        "validation_config": {
            "validate": true,
            "mode": "Strict"
        },
        "extraction_config": {
            "is_batch_event": true,
            "extraction_key": "events",
            "dedup_config": {
                "drop_duplicates": true,
                "dedup_key": "id",
                "dedup_period": 604800
            }
        },
        "dedup_config": {
            "drop_duplicates": true,
            "dedup_key": "mid",
            "dedup_period": 604800
        },
        "data_schema": {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": {
                "eid": {
                    "type": "string"
                },
                "ver": {
                    "type": "string"
                },
                "required": [
                    "eid"
                ]
            },
            "additionalProperties": true
        },
        "router_config": {
            "topic": ""
        },
        "denorm_config": {
            "redis_db_host": "localhost",
            "redis_db_port": 6379,
            "denorm_fields": [
                {
                    "denorm_key": "actor.id",
                    "denorm_out_field": "userdata"
                }
            ]
        },
        "dataset_config": {
            "data_key": "eid",
            "timestamp_key": "ets",
            "entry_topic": "local.ingest",
            "redis_db_host": "localhost",
            "redis_db_port": 6379,
            "index_data": true,
            "redis_db": 0
        },
        "tags": [
            "tag1",
            "tag2"
        ],
        "status": "Live",
        "data_version": 1,
        "created_by": "SYSTEM",
        "updated_by": "SYSTEM",
        "created_date": "",
        "update_date": "",
        "published_date": ""
    },
    TRANSFORMATIONS_SCHEMA: [
        {
            "field_key": "eid",
            "transformation_function": {
                "type": "mask",
                "expr": "eid",
                "condition": null
            },
            "mode": "Strict",
            "metadata": {
                "_transformationType": "mask",
                "_transformedFieldDataType": "string",
                "_transformedFieldSchemaType": "string",
                "section": "transformation"
            }
        }
    ],
    DATASOURCE_SCHEMA:{
            "id": "sb-telemetry_sb-telemetry",
            "datasource": "sb-telemetry",
            "dataset_id": "sb-telemetry",
            "ingestion_spec": {"type":"kafka","spec":{"dataSchema":{"dataSource":"dataset-conf_day","dimensionsSpec":{"dimensions":[{"type":"string","name":"a"},{"type":"string","name":"obsrv.meta.source.connector"},{"type":"string","name":"obsrv.meta.source.id"}]},"timestampSpec":{"column":"obsrv_meta.syncts","format":"auto"},"metricsSpec":[],"granularitySpec":{"type":"uniform","segmentGranularity":"DAY","queryGranularity":"none","rollup":false}},"tuningConfig":{"type":"kafka","maxBytesInMemory":134217728,"maxRowsPerSegment":5000000,"logParseExceptions":true},"ioConfig":{"type":"kafka","consumerProperties":{"bootstrap.servers":"localhost:9092"},"taskCount":1,"replicas":1,"taskDuration":"PT1H","useEarliestOffset":true,"completionTimeout":"PT1H","inputFormat":{"type":"json","flattenSpec":{"useFieldDiscovery":true,"fields":[{"type":"path","expr":"$.['a']","name":"a"},{"type":"path","expr":"$.obsrv_meta.['syncts']","name":"obsrv_meta.syncts"},{"type":"path","expr":"$.obsrv_meta.source.['connector']","name":"obsrv.meta.source.connector"},{"type":"path","expr":"$.obsrv_meta.source.['connectorInstance']","name":"obsrv.meta.source.id"},{"expr":"$.obsrv_meta.syncts","name":"obsrv_meta.syncts","type":"path"}]}},"appendToExisting":false}}},
            "datasource_ref": "sb-telemetry_DAY",
            "retention_period": {
                "enabled": "false"
            },
            "archival_policy": {
                "enabled": "false"
            },
            "purge_policy": {
                "enabled": "false"
            },
            "backup_config": {
                "enabled": "false"
            },
            "status": "Live",
            "created_by": "SYSTEM",
            "updated_by": "SYSTEM",
            "published_date": "2023-07-03 00:00:00"
    }
}