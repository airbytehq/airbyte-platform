import { z } from "zod";

import { DestinationOperation } from "core/api/types/AirbyteClient";
import { trackError } from "core/utils/datadog";
import { MAX_ARRAY_LENGTH } from "core/utils/zod";

export function getRequiredFields(destinationOperation: DestinationOperation) {
  // Ensures we're dealing with a string of arrays, since the spec's generated TS type does not guarantee this
  const parseResult = z
    .string()
    .array()
    .max(MAX_ARRAY_LENGTH)
    .optional()
    .safeParse(destinationOperation.schema?.required);
  if (parseResult.success) {
    return parseResult.data ?? [];
  }
  trackError(new Error(`Failed to parse required fields for operation ${destinationOperation.objectName}`), {
    destinationOperation,
  });
  return [];
}
