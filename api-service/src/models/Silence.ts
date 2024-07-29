import { DataTypes } from "sequelize";
import { sequelize } from "../connections/databaseConnection";
import { v4 as uuid } from 'uuid';

export const Silence = sequelize.define("silences", {
    id: {
        type: DataTypes.STRING,
        defaultValue: () => uuid().toString(),
        primaryKey: true
    },
    manager: {
        type: DataTypes.STRING,
    },
    start_time: {
        type: DataTypes.DATE,
    },
    end_time: {
        type: DataTypes.DATE,
    },
    alert_id: {
        type: DataTypes.STRING,
    },
    created_by: {
        type: DataTypes.STRING,
        defaultValue: "ADMIN",
    },
    updated_by: {
        type: DataTypes.STRING,
        defaultValue: "ADMIN",
    },
    context: {
        type: DataTypes.JSON,
        defaultValue: {},
    }
}, {
    tableName: "silences",
    timestamps: true,
});