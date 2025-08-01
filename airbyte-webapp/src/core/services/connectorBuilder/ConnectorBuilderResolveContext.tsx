import merge from "lodash/merge";
import { createContext, useCallback, useContext, useMemo, useState } from "react";
import { useIntl } from "react-intl";
import { useParams } from "react-router-dom";
import { useMount } from "react-use";

import { LoadingPage } from "components";
import { DEFAULT_JSON_MANIFEST_VALUES_WITH_STREAM } from "components/connectorBuilder/constants";
import { getStreamHash } from "components/connectorBuilder/useStreamTestMetadata";
import { convertJsonToYaml, getStreamName } from "components/connectorBuilder/utils";

import { HttpError, useBuilderProject, useResolveManifest } from "core/api";
import { ConnectorBuilderProjectRead } from "core/api/types/AirbyteClient";
import { KnownExceptionInfo, ResolveManifest } from "core/api/types/ConnectorBuilderClient";
import { ConnectorManifest } from "core/api/types/ConnectorManifest";

interface ResolveContext {
  projectId: string;
  builderProject: ConnectorBuilderProjectRead;
  initialYaml: string;
  initialResolvedManifest: ConnectorManifest | null;
  resolveManifest: (manifestToResolve: ConnectorManifest) => Promise<ResolveManifest>;
  resolveError: HttpError<KnownExceptionInfo> | null;
  resolveErrorMessage: string | undefined;
  isResolving: boolean;
  resetResolveState: () => void;
}

export const ConnectorBuilderResolveContext = createContext<ResolveContext | null>(null);

export const ConnectorBuilderResolveProvider: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => {
  const { formatMessage } = useIntl();
  const { projectId } = useParams<{
    projectId: string;
  }>();
  if (!projectId) {
    throw new Error("Could not find project id in path");
  }

  const builderProject = useBuilderProject(projectId);
  const persistedManifest = useMemo(
    () =>
      (builderProject.declarativeManifest?.manifest as ConnectorManifest) ?? DEFAULT_JSON_MANIFEST_VALUES_WITH_STREAM,
    [builderProject.declarativeManifest?.manifest]
  );

  const {
    resolveManifest: resolveManifestMutation,
    isResolving,
    resolveError,
    resetResolveState,
  } = useResolveManifest();
  const resolveManifest = useCallback(
    async (manifestToResolve: ConnectorManifest) => {
      return resolveManifestMutation({ manifestToResolve, projectId });
    },
    [resolveManifestMutation, projectId]
  );
  const unknownErrorMessage = formatMessage({ id: "connectorBuilder.unknownError" });
  const resolveErrorMessage = useMemo(
    () =>
      !!resolveError
        ? resolveError instanceof HttpError
          ? resolveError.response?.message || unknownErrorMessage
          : unknownErrorMessage
        : undefined,
    [resolveError, unknownErrorMessage]
  );

  // undefined -> resolve call has not yet completed
  // null -> resolve call completed with an error
  // ConnectorManifest -> resolve call completed successfully
  const [initialResolvedManifest, setInitialResolvedManifest] = useState<ConnectorManifest | null | undefined>(
    undefined
  );
  const [initialYaml, setInitialYaml] = useState<string>("");
  useMount(() => {
    resolveManifest(persistedManifest)
      .then((result) => {
        const resolvedManifest = result.manifest as ConnectorManifest;
        setInitialStreamHashes(persistedManifest, resolvedManifest);
        setInitialResolvedManifest(resolvedManifest);
        setInitialYaml(convertJsonToYaml(resolvedManifest));
        return result;
      })
      .catch(() => {
        setInitialResolvedManifest(null);
        setInitialYaml(convertJsonToYaml(persistedManifest));
      });
  });

  if (initialResolvedManifest === undefined) {
    return <LoadingPage />;
  }

  return (
    <ConnectorBuilderResolveContext.Provider
      value={{
        projectId,
        builderProject,
        initialYaml,
        initialResolvedManifest,
        resolveManifest,
        isResolving,
        resolveError,
        resolveErrorMessage,
        resetResolveState,
      }}
    >
      {children}
    </ConnectorBuilderResolveContext.Provider>
  );
};

export const useConnectorBuilderResolve = (): ResolveContext => {
  const connectorBuilderResolve = useContext(ConnectorBuilderResolveContext);
  if (!connectorBuilderResolve) {
    throw new Error("useConnectorBuilderResolve must be used within a ConnectorBuilderResolveProvider.");
  }

  return connectorBuilderResolve;
};

/**
 * Sets the hash of the resolved streams in the testedStreams metadata on both the persisted and resolved manifest,
 * for any streams which aren't already in testedStreams.
 *
 * The reason for this is that connectors built outside of the builder likely have already been tested in their own way,
 * and we don't want to require users who are making changes to those connectors to have to re-test those streams in
 * order to contribute their changes.
 *
 * With this, we will only require testing streams that the user changes.
 */
function setInitialStreamHashes(persistedManifest: ConnectorManifest, resolvedManifest: ConnectorManifest) {
  if (!persistedManifest.streams || !resolvedManifest.streams) {
    return;
  }
  if (persistedManifest.streams.length !== resolvedManifest.streams.length) {
    // this should never happen, since resolving a manifest should never affect the number of streams
    throw new Error("Persisted manifest streams length doesn't match resolved streams length");
  }
  resolvedManifest.streams.forEach((resolvedStream, i) => {
    const streamName = getStreamName(resolvedStream, i);
    if ((persistedManifest.metadata?.testedStreams as Record<string, unknown>)?.[streamName]) {
      return;
    }
    const streamHash = getStreamHash(resolvedStream);
    const updatedMetadata = merge({}, persistedManifest.metadata, {
      testedStreams: {
        [streamName]: {
          streamHash,
        },
      },
    });
    persistedManifest.metadata = updatedMetadata;
    resolvedManifest.metadata = updatedMetadata;
  });
}
