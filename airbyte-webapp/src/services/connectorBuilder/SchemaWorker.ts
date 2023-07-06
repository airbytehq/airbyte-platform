import { Validator } from "@cfworker/json-schema";
import uniq from "lodash/uniq";

import { StreamReadSlicesItemPagesItemRecordsItem } from "core/api/types/ConnectorBuilderClient";
import { InlineSchemaLoaderSchema } from "core/api/types/ConnectorManifest";

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
  const validator = new Validator(parsedSchema, undefined, false);
  const errors = uniq(
    records.flatMap((record) =>
      validator.validate(record).errors.map((error) => `${error.error} (${error.keywordLocation})`)
    )
  );
  const response: OutgoingData = { incompatibleSchemaErrors: errors.length ? errors : undefined, streamName };
  postMessage(response);
};
