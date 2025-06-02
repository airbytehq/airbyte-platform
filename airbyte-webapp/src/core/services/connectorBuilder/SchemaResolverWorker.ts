import { resolveRefs } from "../../../components/forms/SchemaForm/utils";

export interface IncomingSchema {
  schema: object;
}

export interface OutgoingSchema {
  resolvedSchema: object;
}

onmessage = async (event: MessageEvent<IncomingSchema>) => {
  const { schema } = event.data;

  const resolvedSchema = resolveRefs(schema);
  postMessage({
    resolvedSchema,
  });
};
