import _ from "lodash";
import { config } from "../../configs/Config";
import { UniqueValues, FieldSchema, RollupSummary } from "../../types/SchemaModel";


export const generateRollupSummary = (uniqueValues: UniqueValues) => {
    const summary: RollupSummary = {};
    Object.entries(uniqueValues).map(([field, value]) => {
        let data: Record<string, any> = {};
        _.map(value, (item: string) => {
            if (!_.has(data, [field, item])) _.set(data, [field, item], 1);
            else data[field][item] += 1;
        });
        const resultData: Record<string, any> = {};
        _.map(_.keys(data), (path: string) => {
            Object.entries(data[path]).map(([key, value]: any) => {
                const totalValue = _.sum(_.values(data[path]));
                const ratio = Math.round((value / totalValue) * 100);
                if (!_.has(resultData, path)) _.set(resultData, path, ratio);
                else if (ratio > _.get(resultData, path))
                    _.set(resultData, path, ratio);
            });
            let fieldName = parseSchemaPath(path);
            summary[fieldName] = {
                path: `$.${path}`,
                cardinality: 100 - _.get(resultData, path),
                index: _.get(resultData, path) >= config.rollup_ratio,
            };
        });
    });
    return { summary };
};

export const updateCardinalColumns = (data: Record<string, any>, fieldSchema: FieldSchema, path: string, key: string, uniqueValues: UniqueValues) => {
    const schemaPath = generateSchemaPath(path, key);
    if (_.includes(config.unique_formats, fieldSchema.format)) {
        const value = _.get(data, key);
        if (!_.has(uniqueValues, [schemaPath])) uniqueValues[schemaPath] = [value];
        else uniqueValues[schemaPath].push(value);
    }
    return uniqueValues;
};

export const generateSchemaPath = (path: string, key: string) => {
    return (path ? `${path}.properties.${key}` : key).replace(/\[\*\]/gi, ".items");
};

export const parseSchemaPath = (path: string) => {
    return path.replace("$.", "").replace(/.properties/gi, "").replace(/.items/gi, "[*]");
};
