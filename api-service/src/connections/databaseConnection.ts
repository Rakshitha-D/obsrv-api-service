import { Sequelize } from "sequelize";
import { connectionConfig } from "../configs/ConnectionsConfig"

const { database, host, password, port, username } = connectionConfig.postgres

export const sequelize = new Sequelize({
    database, password, username: username, dialect: "postgres", host, port: +port, pool: {
        max: 2,
        min: 1,
        acquire: 30000,
        idle: 10000
      }
})

 export const health = async () => {
    return sequelize.query("select 1")
}

export const query = async (query: string) => {
    const [results, metadata] = await sequelize.query(query)
    return {
        results, metadata
    }
}