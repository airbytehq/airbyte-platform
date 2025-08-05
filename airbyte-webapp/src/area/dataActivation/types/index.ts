import { z } from "zod";

import { DestinationSyncMode, SyncMode } from "core/api/types/AirbyteClient";

import { additionalMappersSchema } from "../utils";

export interface DataActivationStream {
  sourceStreamDescriptor: {
    name: string;
    namespace?: string;
  };
  destinationObjectName: string;
  sourceSyncMode: SyncMode | null;
  destinationSyncMode: DestinationSyncMode | null;
  matchingKeys: string[] | null;
  cursorField: string | null;
  fields: DataActivationField[];
}

export interface DataActivationField {
  sourceFieldName: string;
  destinationFieldName: string;
  additionalMappers?: Array<z.infer<typeof additionalMappersSchema>>;
}

export interface DataActivationConnectionFormValues {
  streams: DataActivationStream[];
}
