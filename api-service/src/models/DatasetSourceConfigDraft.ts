import { DataTypes } from "sequelize";
import { sequelize } from "../connections/databaseConnection";
import { DatasetStatus } from "../types/DatasetModels";

export const DatasetSourceConfigDraft = sequelize.define("dataset_source_config_draft", {
    id: {
        type: DataTypes.STRING,
        primaryKey: true
    },
    dataset_id: {
        type: DataTypes.STRING,
    },
    connector_type: {
        type: DataTypes.STRING,
    },
    connector_config: {
        type: DataTypes.JSON,
        defaultValue: {}
    },
    status: {
        type: DataTypes.STRING,
        defaultValue: DatasetStatus.Draft
    },
    connector_stats: {
        type: DataTypes.JSON,
        defaultValue: {}
    },
    created_by: {
        type: DataTypes.STRING,
        defaultValue: "SYSTEM",
    },
    updated_by: {
        type: DataTypes.STRING,
        defaultValue: "SYSTEM",
    }
}, {
    tableName: "dataset_source_config_draft",
    timestamps: true,
    createdAt: "created_date",
    updatedAt: "updated_date",
})