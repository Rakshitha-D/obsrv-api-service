import * as _ from "lodash";

export class SchemaMerger {

    mergeSchema(schema: Map<string, any>[]): any {
        try {
            let data = {};
            _.map(schema, (item: any) => {
                data = _.merge(data, item)
            });
            return data;
        }
        catch (error) {
            console.log(error)
        }
    }

}