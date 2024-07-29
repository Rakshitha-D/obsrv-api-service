export interface DataSetConfig {
    querying: Record<string, any>
    indexConfiguration: Record<string, any>
    processing: Record<string, any>
  }
  

export interface DatasetSchemeRequest {
    data: Map<string, any>[],
    config: DatasetSchemaConfig
}

export interface DatasetSchemaConfig {
    dataset: string,
    isBatch?: boolean
    extractionKey: string,
}

export interface DatasetSchemaResponse {
    schema: any;
    configurations: DataSetConfig
    dataMappings: Record<string, any>
}

export interface DataSetConfig {
    querying: Record<string, any>
    indexConfiguration: Record<string, any>
    processing: Record<string, any>
  }


export interface DatasetSchemaResponse {
    schema: any;
    configurations: DataSetConfig
    dataMappings: Record<string, any>
}

export interface DatasetSchemeRequest {
    data: Map<string, any>[],
    config: DatasetSchemaConfig
}

export interface DatasetSchemaConfig {
    dataset: string,
    isBatch?: boolean
    extractionKey: string,
}

export interface SuggestionsTemplate {
    property: string;
    suggestions: Suggestion[];
}

export interface Suggestion {
    message: string;
    arrivalConflict?: boolean;
    advice: string;
    resolutionType: string;
    severity: string;
}

export interface ConflictTypes {
    schema: Conflict;
    required: Conflict;
    formats: Conflict;
    absolutePath: string;
}

export interface Conflict {
    property: string,
    type: string,
    path: string,
    conflicts: any,
    values: any[],
    resolution: any,
    severity: string
}


export interface FlattenSchema {
    property: string
    dataType: string
    isRequired: boolean
    path: string | any;
    absolutePath: string
    formate: string
}

export interface Occurance {
    property: { [key: string]: number };
    dataType: { [key: string]: number };
    isRequired: { [key: string]: number };
    path: { [key: string]: number };
    absolutePath: { [key: string]: number };
    format: { [key: string]: number };
}

export interface UniqueValues {
    [key: string]: any[];
}

export interface FieldSchema {
    type?: string | any;
    format?: string
    properties?: Record<string, FieldSchema>;
    items?: FieldSchema;
}

export interface RollupSummary {
    [key: string]: {
        path: string;
        cardinality: number;
        index: boolean;
    };
}
