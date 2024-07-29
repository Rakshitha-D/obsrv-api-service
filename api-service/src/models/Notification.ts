import { DataTypes } from "sequelize";
import { sequelize } from "../connections/databaseConnection";
import { v4 } from 'uuid';

export const Notification = sequelize.define("notificationchannel", {
    id: {
        type: DataTypes.STRING,
        defaultValue: () => v4().toString(),
        primaryKey: true
    },
    manager: {
        type: DataTypes.STRING
    },
    name: {
        type: DataTypes.STRING,
        unique: true,
    },
    status: {
        type: DataTypes.ENUM("draft", "live", "retired"),
        defaultValue: "draft",
    },
    type: {
        type: DataTypes.ENUM("Slack"),
        defaultValue: "Slack",
    },
    config: {
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
    },
    context: {
        type: DataTypes.JSON,
        defaultValue: {},
      }
}, {
    tableName: "notificationChannel"
});


