import _, { isUndefined } from "lodash";

export class SchemaArrayValidator {
    public validate(schemas: any) {
        _.map(schemas, (schema: any, index: number) => {
            Object.entries(schema).map(([schemaKey, schemaValue]) => {
                if (typeof schemaValue === 'object') {
                    this.handleNestedObject(index, `${schemaKey}`, schemaValue, schemas);
                }
            });
        });
        return schemas
    }

    private checkForInvalidArray(value: any) {
        if (_.has(value, 'items') && _.has(value, 'properties'))
            _.unset(value, 'properties');
    }

    private handleNestedObject(index: any, path: string, value: any, schemas: any) {
        Object.entries(value).map(([nestedKey, nestedValue]: any) =>  {
            if (typeof nestedValue === 'object') {
                this.handleNestedObject(index, `${path}.${nestedKey}`, nestedValue, schemas)
            } else if (nestedValue.type === 'array' && (nestedValue.items != false)) {
                this.checkForInvalidArray(nestedValue);
                let isValidArray = true;
                if(_.isEqual(_.get(schemas[0], `${path}.${nestedKey}.type`), _.get(schemas[index], `${path}.${nestedKey}.type`))) {
                    isValidArray = _.isEqual(
                        _.get(schemas[0], `${path}.${nestedKey}.items`),
                        _.get(schemas[index], `${path}.${nestedKey}.items`)
                    )
                }
                if (!isValidArray) {
                    this.deleteItemsAndSetAdditionalProperties(schemas, `${path}.${nestedKey}`)
                }
            } else if (nestedValue.type === 'array' && (nestedValue.items == false)) {
                this.deleteItemsAndSetAdditionalProperties(schemas, `${path}.${nestedKey}`)
            }
        })
    }

    private deleteItemsAndSetAdditionalProperties(schemas: any, path: string) {
        _.map((schemas), (schema: any, index: number) => {
            if (!isUndefined(_.get(schema, path))) {
                _.unset(schema, `${path}`)
                _.set(schema, `${path}.type`, "array");
                _.set(schema, `${path}.additionalProperties`, false);
            }
        });
    }

}
