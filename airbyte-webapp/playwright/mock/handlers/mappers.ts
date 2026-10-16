import type { HandlerContext, HandlerMap } from "./handler";
import type { FieldSpec } from "@src/core/api/types/AirbyteClient";

import { notFound } from "./handler";

/**
 * Answers a mapper validation from the request: every mapper the webapp sent is valid, and no mapper changes the
 * fields. The server reads the fields of the stream from its schema and applies each mapper to them
 * (WebBackendMappersHandler.kt). The mock does not read the stream schema and does not apply the mappers, so the test
 * gives the fields of the stream. Example:
 *
 *   airbyte.override("webBackendValidateMappers", mapperValidation.allValid(usersFields));
 *
 * A stream that the connection does not have fails the test, as the server answers it with an error.
 */
function allValid(fields: FieldSpec[]) {
  return ({ body, store }: HandlerContext<"webBackendValidateMappers">) => {
    const connection = store.connections.find(body.connectionId);
    if (!connection) {
      return notFound();
    }
    const { namespace, name } = body.streamDescriptor;
    const hasStream = connection.syncCatalog.streams.some(
      // The webapp sends no namespace for a stream without one. The catalog can hold null for it.
      ({ stream }) => (stream?.namespace ?? undefined) === (namespace ?? undefined) && stream?.name === name
    );
    if (!hasStream) {
      throw new Error(`Connection ${body.connectionId} has no stream ${[namespace, name].filter(Boolean).join(".")}`);
    }

    return {
      initialFields: fields,
      outputFields: fields,
      mappers: body.mappers.map(({ id }) => ({ id, inputFields: fields, outputFields: fields })),
    };
  };
}

/** Answers for the mapper validation of the Mappings page. */
export const mapperValidation = { allValid };

/**
 * Without fields from the test, every stream has no fields. A page with saved mappers still loads, but the webapp
 * offers no field to pick.
 */
export const mapperHandlers: HandlerMap = {
  webBackendValidateMappers: allValid([]),
};
