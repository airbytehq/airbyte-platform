import Validator from "jsonschema";
import uniq from "lodash/uniq";

import { humanReadableError } from "components/connectorBuilder/humanReadableValidationError";

import type { StreamReadSlicesItemPagesItemRecordsItem } from "core/api/types/ConnectorBuilderClient";
import type { InlineSchemaLoaderSchema } from "core/api/types/ConnectorManifest";

export interface IncomingData {
  schema: string;
  records: StreamReadSlicesItemPagesItemRecordsItem[];
  streamName: string;
}

export interface OutgoingData {
  incompatibleSchemaErrors?: string[];
  streamName: string;
}

onmessage = (event: MessageEvent<IncomingData>) => {
  const { schema, records, streamName } = event.data;
  let parsedSchema: InlineSchemaLoaderSchema;
  try {
    parsedSchema = JSON.parse(schema);
  } catch {
    // if the schema is not valid JSON, we can't validate it
    postMessage({ streamName });
    return;
  }

  const errors = uniq(
    records.flatMap((record) => Validator.validate(record, parsedSchema).errors.map(humanReadableError))
  );

  const response: OutgoingData = { incompatibleSchemaErrors: errors.length ? errors : undefined, streamName };

  postMessage(response);
};
