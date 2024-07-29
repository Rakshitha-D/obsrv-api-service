import { DataTypes } from "sequelize";
import { sequelize } from "../connections/databaseConnection";
import { v4 as uuidv4 } from 'uuid';

export const Alert = sequelize.define("alerts", {
  id: {
    type: DataTypes.STRING,
    defaultValue: () => uuidv4().toString(),
    primaryKey: true
  },
  manager: {
    type: DataTypes.STRING,
  },
  name: {
    type: DataTypes.STRING,
    unique: true,
  },
  status: {
    type: DataTypes.ENUM("draft", "live", "retired"),
    defaultValue: "draft",
  },
  description: {
    type: DataTypes.STRING,
  },
  expression: {
    type: DataTypes.STRING,
  },
  severity: {
    type: DataTypes.ENUM("warning", "critical"),
    defaultValue: "warning"
  },
  category: {
    type: DataTypes.STRING,
  },
  annotations: {
    type: DataTypes.JSON,
    defaultValue: {}
  },
  labels: {
    type: DataTypes.JSON,
    defaultValue: {}
  },
  frequency: {
    type: DataTypes.STRING,
    defaultValue: "1m",
  },
  interval: {
    type: DataTypes.STRING,
    defaultValue: "1m",
  },
  metadata: {
    type: DataTypes.JSON,
    defaultValue: {},
  },
  created_by: {
    type: DataTypes.STRING,
    defaultValue: "SYSTEM",
  },
  updated_by: {
    type: DataTypes.STRING,
    defaultValue: "SYSTEM",
  },
  notification: {
    type: DataTypes.JSON,
    defaultValue: {},
  },
  context: {
    type: DataTypes.JSON,
    defaultValue: {},
  }
}, {
  tableName: "alerts"
});

