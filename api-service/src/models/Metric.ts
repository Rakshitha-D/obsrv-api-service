import { DataTypes } from "sequelize";
import { sequelize } from "../connections/databaseConnection";
import { v4 as uuidv4 } from 'uuid';

export const Metrics = sequelize.define("metrics", {
  id: {
    type: DataTypes.STRING,
    defaultValue: () => uuidv4().toString(),
    primaryKey: true
  },
  alias: {
    type: DataTypes.STRING,
    unique: true,
  },
  component: {
    type: DataTypes.STRING,
  },
  subComponent: {
    type: DataTypes.STRING,
  },
  metric: {
    type: DataTypes.STRING,
  },
  context: {
    type: DataTypes.JSON,
    defaultValue: {}
  }
}, {
  tableName: "metrics"
});