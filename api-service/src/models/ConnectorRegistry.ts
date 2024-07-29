import { DataTypes } from "sequelize";
import { sequelize } from "../connections/databaseConnection";

export const ConnectorRegistry = sequelize.define("connector_registry", {
    id: {
        type: DataTypes.STRING,
        primaryKey: true
    },
    connector_id: {
        type: DataTypes.STRING,
        allowNull: false,
        unique: "connector_registry_unique"
    },
    name: {
        type: DataTypes.STRING,
        allowNull: false
    },
    type: {
        type: DataTypes.STRING,
        allowNull: false
    },
    category: {
        type: DataTypes.STRING,
        allowNull: false
    },
    version: {
        type: DataTypes.STRING,
        allowNull: false,
        unique: "connector_registry_unique"
    },
    description: {
        type: DataTypes.STRING
    },
    technology: {
        type: DataTypes.STRING,
        allowNull: false
    },
    runtime: {
        type: DataTypes.STRING,
        allowNull: false
    },
    licence: {
        type: DataTypes.STRING,
        allowNull: false
    },
    owner: {
        type: DataTypes.STRING,
        allowNull: false
    },
    iconURL: {
        type: DataTypes.STRING
    },
    status: {
        type: DataTypes.ENUM("Draft", "InValidation", "Live", "Retired"),
        defaultValue: "Draft",
    },
    ui_spec: {
        type: DataTypes.JSON,
        defaultValue: {}
    },
    source_url: {
        type: DataTypes.STRING,
        allowNull: false
    },
    source: {
        type: DataTypes.STRING,
        allowNull: false
    },
    created_by: {
        type: DataTypes.STRING,
        defaultValue: "SYSTEM"
    },
    updated_by: {
        type: DataTypes.STRING,
        defaultValue: "SYSTEM"
    },
    live_date: {
        type: DataTypes.DATE
    }
}, {
    tableName: "connector_registry",
    timestamps: true,
    createdAt: "created_date",
    updatedAt: "updated_date"
})