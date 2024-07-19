import _ from "lodash"
import { Conflict, ConflictTypes, Suggestion, SuggestionsTemplate } from "../../types/SchemaModel"
import constants from "./Constants.json"
import { SchemaSuggestionTemplate } from "./Template"

export class SuggestionTemplate {

    public createSuggestionTemplate(sample: ConflictTypes[]): SuggestionsTemplate[] {
        return _.map(sample, (value, key) => {
            const dataTypeSuggestions = this.getSchemaMessageTemplate(value.schema)
            const requiredSuggestions = this.getRequiredMessageTemplate(value.required)
            const formatSuggestions = this.getPropertyFormatTemplate(value.formats)
            return <SuggestionsTemplate>{
                "property": value.absolutePath,
                "suggestions": _.reject([dataTypeSuggestions, requiredSuggestions, formatSuggestions], _.isEmpty)
            }
        })
    }

    private getSchemaMessageTemplate(object: Conflict, arrival_format: boolean = false): Suggestion | [] {
        if (_.isEmpty(object)) return <Suggestion>{}
        let message, arrival_format_message, advice;
        if (object.type === constants.SCHEMA_RESOLUTION_TYPE.NULL_FIELD) {
            message = SchemaSuggestionTemplate.getSchemaNullTypeMessage(object.conflicts, object.property);
            advice = SchemaSuggestionTemplate.TEMPLATES.SCHEMA_SUGGESTION.CREATE.NULL_TYPE_PROPERTY.ADVICE;
        } else {
            let { conflictMessage, arrivalFormatMessage } = SchemaSuggestionTemplate.getSchemaDataTypeMessage(object.conflicts, object.property);
            message = conflictMessage;
            arrival_format_message = arrivalFormatMessage;
            advice = SchemaSuggestionTemplate.TEMPLATES.SCHEMA_SUGGESTION.CREATE.DATATYPE_PROPERTY.ADVICE;
        }
    
        const suggestion = <Suggestion>{
            message: message,
            advice,
            resolutionType: object.resolution["type"],
            severity: object.severity,
            path: object.path
        };
        if (arrival_format && arrival_format_message) {
            suggestion["message"] = arrival_format_message;
            suggestion["arrivalConflict"] = true;
            suggestion["resolutionType"] = constants.SCHEMA_RESOLUTION_TYPE.ARRIVAL_FORMAT;
        } else if(arrival_format && !arrival_format_message)
            return [];
        return suggestion;
    }
    

    public getPropertyFormatTemplate(object: Conflict): Suggestion {
        if (_.isEmpty(object)) return <Suggestion>{}
        const conflictMessage = SchemaSuggestionTemplate.getSchemaFormatMessage(object.conflicts, object.property)
        const adviceObj = SchemaSuggestionTemplate.getSchemaFormatAdvice(object.conflicts)
        return <Suggestion>{
            message: conflictMessage,
            advice: adviceObj.advice,
            resolutionType: object.resolution["type"],
            severity: object.severity,
            path: object.path
        }
    }

    public getRequiredMessageTemplate(object: Conflict): Suggestion {
        if (_.isEmpty(object)) return <Suggestion>{}
        const conflictMessage = SchemaSuggestionTemplate.getSchemaRequiredTypeMessage(object.conflicts, object.property)
        return <Suggestion>{
            message: conflictMessage,
            advice: SchemaSuggestionTemplate.TEMPLATES.SCHEMA_SUGGESTION.CREATE.OPTIONAL_PROPERTY.ADVICE,
            resolutionType: object.resolution["type"],
            severity: object.severity,
            path: object.path
        }
    }
}
