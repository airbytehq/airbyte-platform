import { Suspense, useCallback } from "react";

import { SchemaForm } from "components/forms/SchemaForm/SchemaForm";
import { LogoAnimation } from "components/LoadingPage/LogoAnimation";
import { FlexContainer } from "components/ui/Flex";

import { IncomingSchema, OutgoingSchema } from "core/services/connectorBuilder/SchemaResolverWorker";
import SchemaResolverWorker from "core/services/connectorBuilder/SchemaResolverWorker?worker";

import styles from "./GeneratedStreamView.module.scss";
import { StreamConfigView } from "./StreamConfigView";
import declarativeComponentSchema from "../../../../build/declarative_component_schema.yaml";
import { StreamId, getStreamFieldPath } from "../types";

const worker = new SchemaResolverWorker();

let cachedResolvedSchema: object | null = null;
let resolvedSchemaPromise: Promise<object> | null = null;

// Use a web-worker to resolve the schema, since it is a large file
// and we don't want to block the main thread.
const getResolvedSchema = (): object => {
  if (cachedResolvedSchema) {
    return cachedResolvedSchema;
  }

  if (!resolvedSchemaPromise) {
    resolvedSchemaPromise = new Promise((resolve) => {
      // Setup the worker message handler
      worker.onmessage = (event: MessageEvent<OutgoingSchema>) => {
        const { resolvedSchema } = event.data;
        cachedResolvedSchema = resolvedSchema;
        resolve(resolvedSchema);
      };

      // Post the message to the worker
      worker.postMessage({ schema: declarativeComponentSchema } as IncomingSchema);

      // Suspend rendering while we wait
      throw resolvedSchemaPromise;
    });
  }

  throw resolvedSchemaPromise;
};

export const GeneratedStreamView: React.FC<{ streamId: StreamId; scrollToTop: () => void }> = ({
  streamId,
  scrollToTop,
}) => {
  return (
    <Suspense
      fallback={
        <FlexContainer className={styles.loadingContainer} justifyContent="center" alignItems="center">
          <LogoAnimation />
        </FlexContainer>
      }
    >
      <GeneratedStreamForm streamId={streamId} scrollToTop={scrollToTop} />
    </Suspense>
  );
};

const GeneratedStreamForm = ({ streamId, scrollToTop }: { streamId: StreamId; scrollToTop: () => void }) => {
  // Resolve the schema so that we can use a sub-schema in it for SchemaForm below
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const resolvedSchema = getResolvedSchema() as Record<string, any>;
  const streamFieldPath = useCallback((fieldPath?: string) => getStreamFieldPath(streamId, fieldPath), [streamId]);

  return (
    <SchemaForm
      key={streamFieldPath()}
      schema={resolvedSchema.definitions.DeclarativeStream}
      nestedUnderPath={streamFieldPath()}
      disableFormControls
      disableValidation
    >
      <StreamConfigView streamId={streamId} scrollToTop={scrollToTop} />
    </SchemaForm>
  );
};
