import _ from "lodash";
import { Conflict } from "../../types/SchemaModel";
import constants from "./Constants.json";
import { dataMappingPaths } from "./../SchemaGenerateService/SchemaHandler";

export const SchemaSuggestionTemplate = {
    TEMPLATES: {
        SCHEMA_SUGGESTION: {
            CREATE: {
                OPTIONAL_PROPERTY: {
                    MESSAGE: "Conflict in the Schema Generation",
                    ADVICE: "The Property looks to be Optional. System has updated the property schema to optional",
                    SEVERITY: constants.SEVERITY.MEDIUM
                },
                DATATYPE_PROPERTY: {
                    MESSAGE: "Conflict in the Schema Generation",
                    ADVICE: "System can choose highest occurance property or last appeared object property",
                    SEVERITY: constants.SEVERITY["MUST-FIX"]
                },
                NULL_TYPE_PROPERTY: {
                    MESSAGE: "Conflict in the Schema Generation",
                    ADVICE: "The Property has a 'null' data type. 'null' is not a valid data type. Please review and update the property schema accordingly.",
                    SEVERITY: constants.SEVERITY["MUST-FIX"]
                },
                FORMAT_PROPERTY: {
                    MESSAGE: "The Property",
                    DATE_ADVICE: {
                        MESSAGE: "The System can index all data on this column",
                        SEVERITY: constants.SEVERITY.HIGH
                    },
                    DATETIME_ADVICE: {
                        MESSAGE: "The System can index all data on this column",
                        SEVERITY: constants.SEVERITY.LOW
                    },
                    UUID_ADVICE: {
                        MESSAGE: "Suggest to not to index the high cardinal columns",
                        SEVERITY: constants.SEVERITY.LOW
                    },
                    IPV4_ADVICE: {
                        MESSAGE: "Suggest to Mask the Personal Information",
                        SEVERITY: constants.SEVERITY.LOW
                    },
                    IPV6_ADVICE: {
                        MESSAGE: "Suggest to Mask the Personal Information",
                        SEVERITY: constants.SEVERITY.LOW
                    },
                    EMAIL_ADVICE: {
                        MESSAGE: "Suggest to Mask the Personal Information",
                        SEVERITY: constants.SEVERITY.LOW
                    }
                }
            },
            UPDATE: {
                REQUIRED_PROPERTY: {
                    ADVICE: "Might Required to reprocess the existing data"
                },
                DATATYPE_PROPERTY: {
                    ADVICE: "Might Required to reprocess the existing data"
                }
            }

        }
    },

    getSchemaDataTypeMessage(conflicts: Conflict, property: string) {
        const updatedConflicts: any = {};
        _.map((conflicts), (value, key) => {
            const path: any = _.get(dataMappingPaths, key);
            const types = _.split(path, ".");
            if (path && _.size(types) > 0 && !_.keys(updatedConflicts).includes(types[0])) {
                updatedConflicts[types[0]] = value;
            }
        });
        let response: Record<string, null | string> = {
            conflictMessage: _.template(
                `${this.TEMPLATES.SCHEMA_SUGGESTION.CREATE.DATATYPE_PROPERTY.MESSAGE} at property: '${property}'. The property type <% _.map(conflicts, (value, key, list) => { %><%= key %>: <%= value %> time(s)<%= _.last(list) === value ? '' : ', ' %><% }); %><%= _.isEmpty(conflicts) ? '' : '' %>`)({ conflicts }),
            arrivalFormatMessage: null,
        };
        if (_.keys(updatedConflicts).length > 1) {
            _.set(response, "arrivalFormatMessage", _.template(`${this.TEMPLATES.SCHEMA_SUGGESTION.CREATE.DATATYPE_PROPERTY.MESSAGE} at property: '${property}'. The property type <% _.map(conflicts, (value, key, list) => { %><%= key %>: <%= value %> time(s)<%= _.last(list) === value ? '' : ', ' %><% }); %><%= _.isEmpty(conflicts) ? '' : '' %>`)({ conflicts: updatedConflicts }));
        }
        return response;
    },

    getSchemaNullTypeMessage(conflicts: Conflict, property: string): string {
        return `${this.TEMPLATES.SCHEMA_SUGGESTION.CREATE.NULL_TYPE_PROPERTY.MESSAGE} at property: '${property}'. ${this.TEMPLATES.SCHEMA_SUGGESTION.CREATE.NULL_TYPE_PROPERTY.ADVICE}`;
    },

    getSchemaFormatMessage(conflicts: Conflict, property: string): string {
        const conflictKey = _.keys(conflicts)[0];
        return `${this.TEMPLATES.SCHEMA_SUGGESTION.CREATE.FORMAT_PROPERTY.MESSAGE} '${property}' appears to be '${conflictKey}' format type.`;
    },

    getSchemaRequiredTypeMessage(conflicts: Conflict, property: string): string {
        const message = _.template(
            `${this.TEMPLATES.SCHEMA_SUGGESTION.CREATE.OPTIONAL_PROPERTY.MESSAGE} at property: '${property}'. The property <% _.map(conflicts, (value, key, list) => { %><%= key %>: only <%= value %> time(s) appeared <%= _.last(list) === value ? '' : '' %><% }); %><%= _.isEmpty(conflicts) ? '' : '' %>`)({ conflicts });
        return message
    },

    getSchemaFormatAdvice(conflicts: Conflict): any {
        const conflictKey = `${_.upperCase(_.replace(_.keys(conflicts)[0], "-", ""))}_ADVICE`.replace(/\s/g, "")
        return {
            "advice": _.get(this.TEMPLATES.SCHEMA_SUGGESTION.CREATE.FORMAT_PROPERTY, `${conflictKey}.MESSAGE`),
        }
    }

}
