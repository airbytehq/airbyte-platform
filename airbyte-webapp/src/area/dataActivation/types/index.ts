import { DestinationSyncMode, SyncMode } from "core/api/types/AirbyteClient";

export interface DataActivationStream {
  sourceStreamDescriptor: {
    name: string;
    namespace?: string;
  };
  destinationObjectName: string;
  sourceSyncMode: SyncMode | null;
  destinationSyncMode: DestinationSyncMode | null;
  primaryKey: string | null;
  cursorField: string | null;
  fields: DataActivationField[];
}

export interface DataActivationField {
  sourceFieldName: string;
  destinationFieldName: string;
}

export interface DataActivationConnectionFormValues {
  streams: DataActivationStream[];
}
