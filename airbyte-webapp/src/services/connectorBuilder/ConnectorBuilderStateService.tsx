import { UseMutateAsyncFunction, UseQueryResult } from "@tanstack/react-query";
import { load } from "js-yaml";
import isEqual from "lodash/isEqual";
import toPath from "lodash/toPath";
import { editor, Position } from "monaco-editor";
import React, { useCallback, useContext, useEffect, useMemo, useRef, useState } from "react";
import { useFormContext, UseFormReturn } from "react-hook-form";
import { useIntl } from "react-intl";
import { useDebounce, useUpdateEffect } from "react-use";

import { WaitForSavingModal } from "components/connectorBuilder/Builder/WaitForSavingModal";
import { CDK_VERSION } from "components/connectorBuilder/cdk";
import { BuilderState, GeneratedDeclarativeStream, isStreamDynamicStream } from "components/connectorBuilder/types";
import { useAutoImportSchema } from "components/connectorBuilder/useAutoImportSchema";
import { useBuilderErrors } from "components/connectorBuilder/useBuilderErrors";
import { useBuilderWatch } from "components/connectorBuilder/useBuilderWatch";
import { useStreamName } from "components/connectorBuilder/useStreamNames";
import { useStreamTestMetadata } from "components/connectorBuilder/useStreamTestMetadata";
import { UndoRedo, useUndoRedo } from "components/connectorBuilder/useUndoRedo";
import { useUpdateTestingValuesOnChange } from "components/connectorBuilder/useUpdateTestingValuesOnChange";
import { convertJsonToYaml, formatJson } from "components/connectorBuilder/utils";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import {
  BuilderProject,
  BuilderProjectPublishBody,
  BuilderProjectWithManifest,
  convertProjectDetailsReadToBuilderProject,
  NewVersionBody,
  StreamReadTransformedSlices,
  useBuilderProjectReadStream,
  useBuilderProjectFullResolveManifest,
  useCurrentWorkspace,
  usePublishBuilderProject,
  useReleaseNewBuilderProjectVersion,
  useUpdateBuilderProject,
  useIsForeignWorkspace,
  useCancelBuilderProjectStreamRead,
  HttpError,
} from "core/api";
import {
  ConnectorBuilderProjectFullResolveResponse,
  ConnectorBuilderProjectTestingValues,
  ConnectorBuilderProjectTestingValuesUpdate,
  KnownExceptionInfo,
  SourceDefinitionIdBody,
} from "core/api/types/AirbyteClient";
import { StreamRead } from "core/api/types/ConnectorBuilderClient";
import {
  ConnectorManifest,
  DeclarativeComponentSchema,
  DeclarativeStreamType,
  AsyncRetrieverType,
  DeclarativeComponentSchemaStreamsItem,
  DeclarativeStream,
  InlineSchemaLoaderType,
  InlineSchemaLoader,
} from "core/api/types/ConnectorManifest";
import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { useConnectorBuilderResolve } from "core/services/connectorBuilder/ConnectorBuilderResolveContext";
import { FeatureItem, useFeature } from "core/services/features";
import { Blocker, useBlocker } from "core/services/navigation";
import { removeEmptyProperties } from "core/utils/form";
import { useIntent } from "core/utils/rbac";
import { useConfirmationModalService } from "hooks/services/ConfirmationModal";
import { useNotificationService } from "hooks/services/Notification";

import { useConnectorBuilderLocalStorage } from "./ConnectorBuilderLocalStorageService";
import { IncomingData, OutgoingData } from "./SchemaWorker";
import SchemaWorker from "./SchemaWorker?worker";

const worker = new SchemaWorker();

export type BuilderView = BuilderState["view"];

export type SavingState = "loading" | "invalid" | "saved" | "error" | "readonly";

export type ConnectorBuilderPermission = "write" | "readOnly" | "adminReadOnly";

export type TestingValuesUpdate = UseMutateAsyncFunction<
  ConnectorBuilderProjectTestingValues,
  Error,
  Omit<ConnectorBuilderProjectTestingValuesUpdate, "builderProjectId" | "workspaceId">,
  unknown
>;

interface FormStateContext {
  customComponentsCode: string | undefined;
  yamlEditorIsMounted: boolean;
  yamlIsValid: boolean;
  yamlIsDirty: boolean;
  savingState: SavingState;
  blockedOnInvalidState: boolean;
  projectId: string;
  currentProject: BuilderProject;
  previousManifestDraft:
    | { manifest: DeclarativeComponentSchema; componentsFileContent: string | undefined }
    | undefined;
  displayedVersion: number | "draft";
  undoRedo: UndoRedo;
  setDisplayedVersion: (
    value: number | "draft",
    manifest: DeclarativeComponentSchema,
    customComponentsCode: string | undefined
  ) => void;
  setYamlIsValid: (value: boolean) => void;
  setYamlIsDirty: (value: boolean) => void;
  setYamlEditorIsMounted: (value: boolean) => void;
  triggerUpdate: () => void;
  publishProject: (options: BuilderProjectPublishBody) => Promise<SourceDefinitionIdBody>;
  releaseNewVersion: (options: NewVersionBody) => Promise<void>;
  toggleUI: (newMode: BuilderState["mode"]) => Promise<void>;
  updateYamlCdkVersion: (currentManifest: ConnectorManifest) => ConnectorManifest;
  assistEnabled: boolean;
  assistSessionId: string;
  setAssistEnabled: (enabled: boolean) => void;
}

interface TestReadLimits {
  recordLimit: number;
  pageLimit: number;
  sliceLimit: number;
}

interface GeneratedStreamLimits {
  streamLimit: number;
}

export interface TestReadContext {
  streamRead: UseQueryResult<StreamReadTransformedSlices, unknown>;
  testReadLimits: {
    recordLimit: number;
    setRecordLimit: (newRecordLimit: number) => void;
    pageLimit: number;
    setPageLimit: (newPageLimit: number) => void;
    sliceLimit: number;
    setSliceLimit: (newSliceLimit: number) => void;
    defaultLimits: TestReadLimits;
  };
  testState: string;
  setTestState: (newState: string) => void;
  schemaWarnings: {
    schemaDifferences: boolean;
    incompatibleSchemaErrors: string[] | undefined;
  };
  queuedStreamRead: boolean;
  queueStreamRead: () => void;
  cancelStreamRead: () => void;
  testStreamRequestType: "sync" | "async";
  generatedStreamsLimits: {
    streamLimit: number;
    setStreamLimit: (newStreamLimit: number) => void;
    defaultGeneratedLimits: GeneratedStreamLimits;
  };
  generateStreams: UseQueryResult<ConnectorBuilderProjectFullResolveResponse, unknown>;
}

interface FormManagementStateContext {
  isTestingValuesInputOpen: boolean;
  setTestingValuesInputOpen: (open: boolean) => void;
  isTestReadSettingsOpen: boolean;
  setTestReadSettingsOpen: (open: boolean) => void;
  handleScrollToField: (ref: React.RefObject<HTMLDivElement>, path: string) => void;
  setScrollToField: (field: string | undefined) => void;
  newUserInputContext: NewUserInputContext | undefined;
  setNewUserInputContext: (context: NewUserInputContext | undefined) => void;
}

interface NewUserInputContext {
  model: editor.ITextModel;
  position: Position;
}

export const ConnectorBuilderFormStateContext = React.createContext<FormStateContext | null>(null);
export const ConnectorBuilderTestReadContext = React.createContext<TestReadContext | null>(null);
export const ConnectorBuilderFormManagementStateContext = React.createContext<FormManagementStateContext | null>(null);
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export const ConnectorBuilderMainRHFContext = React.createContext<UseFormReturn<any, unknown> | null>(null);

export const useConnectorBuilderPermission = () => {
  const restrictAdminInForeignWorkspace = useFeature(FeatureItem.RestrictAdminInForeignWorkspace);
  const { workspaceId } = useCurrentWorkspace();
  const canUpdateConnector = useIntent("UpdateCustomConnector", { workspaceId });
  const isForeignWorkspace = useIsForeignWorkspace();

  let permission: ConnectorBuilderPermission = "readOnly";
  if (canUpdateConnector) {
    permission = restrictAdminInForeignWorkspace && isForeignWorkspace ? "adminReadOnly" : "write";
  }
  return permission;
};

export const ConnectorBuilderFormStateProvider: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => {
  const permission = useConnectorBuilderPermission();

  return (
    <InternalConnectorBuilderFormStateProvider permission={permission}>
      {children}
    </InternalConnectorBuilderFormStateProvider>
  );
};

export const InternalConnectorBuilderFormStateProvider: React.FC<
  React.PropsWithChildren<{ permission: ConnectorBuilderPermission }>
> = ({ children, permission }) => {
  const { projectId, builderProject, isResolving, resolveError, resolveErrorMessage, resetResolveState } =
    useConnectorBuilderResolve();
  const { mutateAsync: updateProject, error: updateError } = useUpdateBuilderProject(projectId);

  const currentProject: BuilderProject = useMemo(
    () => convertProjectDetailsReadToBuilderProject(builderProject.builderProject),
    [builderProject.builderProject]
  );

  const { setStoredMode, isAssistProjectEnabled, setAssistProjectEnabled, getAssistProjectSessionId } =
    useConnectorBuilderLocalStorage();

  const assistEnabled = isAssistProjectEnabled(projectId);
  const setAssistEnabled = useCallback(
    (enabled: boolean) => {
      setAssistProjectEnabled(projectId, enabled);
    },
    [projectId, setAssistProjectEnabled]
  );
  const assistSessionId = getAssistProjectSessionId(projectId);

  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();
  const analyticsService = useAnalyticsService();

  const [displayedVersion, setDisplayedVersion] = useState<number | "draft">(
    builderProject.declarativeManifest?.version ?? "draft"
  );
  const [previousManifestDraft, setPreviousManifestDraft] = useState<
    FormStateContext["previousManifestDraft"] | undefined
  >(undefined);

  const [yamlIsValid, setYamlIsValid] = useState(true);
  const [yamlIsDirty, setYamlIsDirty] = useState(false);
  const [yamlEditorIsMounted, setYamlEditorIsMounted] = useState(true);

  const { setValue } = useFormContext();
  const mode = useBuilderWatch("mode");
  const name = useBuilderWatch("name");
  const yaml = useBuilderWatch("yaml");
  const manifest = removeEmptyProperties(useBuilderWatch("manifest"), true);
  const customComponentsCode = useBuilderWatch("customComponentsCode");
  const { hasErrors } = useBuilderErrors();

  useUpdateEffect(() => {
    setStoredMode(projectId, mode);
  }, [mode]);

  const toggleUI = useCallback(
    async (newMode: BuilderState["mode"]) => {
      if (newMode === "yaml") {
        setValue("yaml", convertJsonToYaml(manifest));
        setYamlIsValid(true);
        setValue("mode", "yaml");
      } else {
        if (resolveError) {
          openConfirmationModal({
            text: "connectorBuilder.toggleModal.text.uiValueAvailable",
            textValues: { error: resolveErrorMessage ?? "" },
            title: "connectorBuilder.toggleModal.title",
            submitButtonText: "connectorBuilder.toggleModal.submitButton",
            onSubmit: () => {
              setValue("mode", "ui");
              resetResolveState();
              closeConfirmationModal();
              analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.DISCARD_YAML_CHANGES, {
                actionDescription: "YAML changes were discarded due to failure when converting from YAML to UI",
              });
            },
          });
          return;
        }
        setValue("mode", "ui");
      }
    },
    [
      setValue,
      manifest,
      resolveError,
      openConfirmationModal,
      resolveErrorMessage,
      resetResolveState,
      closeConfirmationModal,
      analyticsService,
    ]
  );

  const updateYamlCdkVersion = useCallback(
    (currentManifest: ConnectorManifest) => {
      if (mode === "yaml") {
        const newManifest = { ...(load(yaml) as ConnectorManifest), version: CDK_VERSION };
        setValue("yaml", convertJsonToYaml(newManifest));
        return newManifest;
      }
      return currentManifest;
    },
    [mode, setValue, yaml]
  );

  const [persistedState, setPersistedState] = useState<BuilderProjectWithManifest>(() => ({
    manifest,
    name: builderProject.builderProject.name,
    componentsFileContent: builderProject.builderProject.componentsFileContent,
  }));

  const setToVersion = useCallback(
    (version: number | "draft", newManifest: DeclarativeComponentSchema, componentsFileContent: string | undefined) => {
      setPersistedState({ name, manifest: newManifest, componentsFileContent });
      if (displayedVersion === "draft" && version !== "draft") {
        setPreviousManifestDraft({ manifest, componentsFileContent: customComponentsCode });
      } else if (version === "draft") {
        setPreviousManifestDraft(undefined);
      }
      setValue("generatedStreams", {});
      setValue("manifest", newManifest);
      setValue("yaml", convertJsonToYaml(newManifest));
      setValue("customComponentsCode", componentsFileContent || undefined);
      setValue(
        "view",
        newManifest.streams && newManifest.streams.length > 0
          ? { type: "stream", index: 0 }
          : newManifest.dynamic_streams && newManifest.dynamic_streams.length > 0
          ? { type: "dynamic_stream", index: 0 }
          : { type: "global" }
      );
      setDisplayedVersion(version);
    },
    [name, displayedVersion, setValue, manifest, customComponentsCode]
  );

  const { mutateAsync: sendPublishRequest } = usePublishBuilderProject();
  const { mutateAsync: sendNewVersionRequest } = useReleaseNewBuilderProjectVersion();

  const publishProject = useCallback(
    async (options: BuilderProjectPublishBody) => {
      // update the version so that the manifest reflects which CDK version was used to build it
      const updatedManifest = updateYamlCdkVersion(manifest);
      const result = await sendPublishRequest({ ...options, manifest: updatedManifest });
      setDisplayedVersion(1);
      return result;
    },
    [manifest, sendPublishRequest, updateYamlCdkVersion]
  );

  const releaseNewVersion = useCallback(
    async (options: NewVersionBody) => {
      // update the version so that the manifest reflects which CDK version was used to build it
      const updatedManifest = updateYamlCdkVersion(manifest);
      await sendNewVersionRequest({ ...options, manifest: updatedManifest });
      setDisplayedVersion(options.version);
    },
    [manifest, sendNewVersionRequest, updateYamlCdkVersion]
  );

  const savingState = getSavingState(
    manifest,
    yamlIsValid,
    hasErrors(),
    mode,
    name,
    persistedState,
    updateError,
    permission,
    resolveError,
    isResolving,
    customComponentsCode
  );

  const modeRef = useRef(mode);
  modeRef.current = mode;
  const triggerUpdate = useCallback(async () => {
    // don't update if the saving state indicates it can't be saved
    if (savingState !== "loading") {
      return;
    }
    const resolvedProject: BuilderProjectWithManifest = {
      name,
      manifest,
      yamlManifest: convertJsonToYaml(manifest),
      componentsFileContent: customComponentsCode,
    };
    await updateProject(
      mode === "yaml"
        ? {
            name,
            manifest: load(yaml) as ConnectorManifest,
            yamlManifest: yaml,
            componentsFileContent: customComponentsCode,
          }
        : resolvedProject
    );
    setPersistedState(resolvedProject);
  }, [savingState, mode, name, yaml, customComponentsCode, manifest, updateProject]);

  useDebounce(
    () => {
      if (
        isEqual(persistedState.manifest, manifest) &&
        persistedState.name === name &&
        persistedState.componentsFileContent === customComponentsCode
      ) {
        // first run of the hook, no need to update
        return;
      }
      setDisplayedVersion("draft");
      setPreviousManifestDraft(undefined);
      triggerUpdate();
    },
    2000,
    [triggerUpdate, name, manifest]
  );

  const { pendingBlocker, blockedOnInvalidState } = useBlockOnSavingState(savingState);

  const undoRedo = useUndoRedo();

  const ctx: FormStateContext = {
    customComponentsCode,
    yamlEditorIsMounted,
    yamlIsValid,
    yamlIsDirty,
    savingState,
    blockedOnInvalidState,
    projectId,
    currentProject,
    previousManifestDraft,
    displayedVersion,
    undoRedo,
    setDisplayedVersion: setToVersion,
    setYamlIsValid,
    setYamlIsDirty,
    setYamlEditorIsMounted,
    triggerUpdate,
    publishProject,
    releaseNewVersion,
    toggleUI,
    updateYamlCdkVersion,
    setAssistEnabled,
    assistEnabled,
    assistSessionId,
  };

  return (
    <ConnectorBuilderFormStateContext.Provider value={ctx}>
      {pendingBlocker && <WaitForSavingModal pendingBlocker={pendingBlocker} />}
      {children}
    </ConnectorBuilderFormStateContext.Provider>
  );
};

function useBlockOnSavingState(savingState: SavingState) {
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();
  const [pendingBlocker, setPendingBlocker] = useState<undefined | Blocker>();
  const [blockedOnInvalidState, setBlockedOnInvalidState] = useState(false);
  const blocker = useCallback(
    (blocker: Blocker) => {
      if (savingState === "invalid" || savingState === "error") {
        setBlockedOnInvalidState(true);
        openConfirmationModal({
          title: "form.unsavedChangesTitle",
          text: "connectorBuilder.discardChangesConfirmation",
          submitButtonText: "form.discardChanges",
          onSubmit: () => {
            closeConfirmationModal();
            blocker.proceed();
          },
          onCancel: () => setBlockedOnInvalidState(false),
        });
      } else {
        setPendingBlocker(blocker);
      }
    },
    [closeConfirmationModal, openConfirmationModal, savingState]
  );

  useBlocker(blocker, savingState !== "saved" && savingState !== "readonly");

  useEffect(() => {
    if (savingState === "saved" && pendingBlocker) {
      pendingBlocker.proceed();
    }
  }, [savingState, pendingBlocker]);

  return { pendingBlocker, blockedOnInvalidState };
}

function getSavingState(
  currentManifest: ConnectorManifest,
  yamlIsValid: boolean,
  hasFormErrors: boolean,
  mode: BuilderState["mode"],
  name: string | undefined,
  persistedState: { name: string; manifest?: DeclarativeComponentSchema; componentsFileContent?: string },
  updateError: Error | null,
  permission: ConnectorBuilderPermission,
  resolveError: HttpError<KnownExceptionInfo> | null,
  isResolving: boolean,
  currentComponentsFileContent?: string
): SavingState {
  if (updateError) {
    return "error";
  }
  if (!name) {
    return "invalid";
  }
  if (mode === "ui" && hasFormErrors) {
    return "invalid";
  }
  if (mode === "yaml" && (!yamlIsValid || resolveError)) {
    return "invalid";
  }
  if (permission !== "write") {
    return "readonly";
  }
  if (mode === "yaml" && isResolving) {
    return "loading";
  }

  const currentStateIsPersistedState =
    isEqual(persistedState.manifest, currentManifest) &&
    persistedState.name === name &&
    persistedState.componentsFileContent === currentComponentsFileContent;

  if (currentStateIsPersistedState) {
    return "saved";
  }

  return "loading";
}

export const ConnectorBuilderTestReadProvider: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => {
  const workspaceId = useCurrentWorkspaceId();
  const { updateYamlCdkVersion, yamlIsDirty } = useConnectorBuilderFormState();
  const { projectId, isResolving, resolveError } = useConnectorBuilderResolve();
  const { setValue } = useFormContext();
  const manifest = removeEmptyProperties(useBuilderWatch("manifest"), true);
  const mode = useBuilderWatch("mode");
  const view = useBuilderWatch("view");
  const generatedStreams = useBuilderWatch("generatedStreams");
  const testStreamId = useBuilderWatch("testStreamId");
  const customComponentsCode = useBuilderWatch("customComponentsCode");

  useEffect(() => {
    if (view.type === "stream") {
      setValue("testStreamId", { type: "stream", index: view.index });
    } else if (view.type === "dynamic_stream") {
      setValue("testStreamId", { type: "dynamic_stream", index: view.index });
    } else if (view.type === "generated_stream") {
      setValue("testStreamId", view);
    }
  }, [setValue, view]);

  const streamIsDynamic = isStreamDynamicStream(testStreamId);
  let testStream: DeclarativeComponentSchemaStreamsItem | undefined;
  if (streamIsDynamic) {
    const dynamicStream = manifest.dynamic_streams?.[testStreamId.index];
    if (dynamicStream?.components_resolver.type === "HttpComponentsResolver") {
      testStream = {
        type: DeclarativeStreamType.DeclarativeStream,
        name: dynamicStream.name,
        retriever: {
          ...dynamicStream.components_resolver.retriever,
        },
      };
    }
  } else if (testStreamId.type === "generated_stream") {
    testStream = generatedStreams?.[testStreamId.dynamicStreamName]?.[testStreamId.index];
  } else {
    testStream = manifest.streams?.[testStreamId.index];
  }

  const filteredManifest = {
    ...manifest,
    streams: [testStream],
    dynamic_streams: [],
  };
  const streamName = testStream?.name ?? "";

  const DEFAULT_PAGE_LIMIT = 5;
  const DEFAULT_SLICE_LIMIT = 5;
  const DEFAULT_RECORD_LIMIT = 1000;
  const DEFAULT_STREAM_LIMIT = 100;

  const [pageLimit, setPageLimit] = useState(DEFAULT_PAGE_LIMIT);
  const [sliceLimit, setSliceLimit] = useState(DEFAULT_SLICE_LIMIT);
  const [recordLimit, setRecordLimit] = useState(DEFAULT_RECORD_LIMIT);
  const [streamLimit, setStreamLimit] = useState(DEFAULT_STREAM_LIMIT);
  const [testState, setTestState] = useState("");

  const testReadLimits = {
    pageLimit,
    setPageLimit,
    sliceLimit,
    setSliceLimit,
    recordLimit,
    setRecordLimit,
    defaultLimits: {
      recordLimit: DEFAULT_RECORD_LIMIT,
      pageLimit: DEFAULT_PAGE_LIMIT,
      sliceLimit: DEFAULT_SLICE_LIMIT,
    },
  };

  const generatedStreamsLimits = {
    streamLimit,
    setStreamLimit,
    defaultGeneratedLimits: {
      streamLimit: DEFAULT_STREAM_LIMIT,
    },
  };

  const testStateParsed = testState ? JSON.parse(testState) : undefined;
  const testStateArray = testStateParsed && !Array.isArray(testStateParsed) ? [testStateParsed] : testStateParsed;

  const autoImportSchema = useAutoImportSchema(testStreamId);
  const { updateStreamTestResults, getStreamHasCustomType } = useStreamTestMetadata();

  const streamUsesCustomCode = getStreamHasCustomType(testStreamId);

  const resolvedManifestInput = useMemo(
    () => ({
      manifest,
      builderProjectId: projectId,
      workspaceId,
      streamLimit,
    }),
    [manifest, projectId, workspaceId, streamLimit]
  );

  const fullResolveManifest = useBuilderProjectFullResolveManifest(resolvedManifestInput);
  const { registerNotification } = useNotificationService();
  const { formatMessage } = useIntl();
  const doGenerateStreams = useCallback(async () => {
    const resolvedManifest = await fullResolveManifest.refetch();

    if (!resolvedManifest.data?.manifest || resolvedManifest.isError) {
      return;
    }

    const generatedStreamsByDynamicStreamName = (
      (resolvedManifest.data.manifest as ConnectorManifest).streams ?? []
    ).reduce<Record<string, DeclarativeStream[]>>((acc, stream) => {
      if (!("dynamic_stream_name" in stream)) {
        return acc;
      }
      const dynamicStreamKey = (stream as GeneratedDeclarativeStream).dynamic_stream_name;
      if (acc[dynamicStreamKey] == null) {
        acc[dynamicStreamKey] = [];
      }
      acc[dynamicStreamKey].push(stream);
      return acc;
    }, {});

    setValue("generatedStreams", generatedStreamsByDynamicStreamName);

    registerNotification({
      id: "connectorBuilder.generateStreamsSuccess",
      type: "success",
      text: formatMessage({ id: "connectorBuilder.generateStreamsSuccess" }),
    });

    return resolvedManifest;
  }, [setValue, fullResolveManifest, formatMessage, registerNotification]);

  const streamRead = useBuilderProjectReadStream(
    {
      builderProjectId: projectId,
      manifest: filteredManifest,
      customComponentsCode: streamUsesCustomCode ? customComponentsCode : undefined,
      streamName,
      recordLimit,
      pageLimit,
      sliceLimit,
      state: testStateArray,
      workspaceId,
      formGeneratedManifest: mode === "ui",
    },
    testStream,
    (result) => {
      if (!testStream) {
        return;
      }

      if (result.latest_config_update) {
        setValue("testingValues", result.latest_config_update);
      }

      if (mode === "ui" && autoImportSchema && result.inferred_schema) {
        result.inferred_schema.additionalProperties = true;
        const schemaLoader: InlineSchemaLoader = {
          type: "InlineSchemaLoader",
          schema: result.inferred_schema,
        };

        // Set the inferred schema on the manifest when autoImportSchema is enabled
        if (testStreamId.type === "stream") {
          setValue(`manifest.streams.${testStreamId.index}.schema_loader`, schemaLoader, {
            shouldValidate: true,
            shouldTouch: true,
            shouldDirty: true,
          });

          // Set the schema_loader on the test stream to the inferred schema as well, so
          // that it is included in the stream when generating the test result stream hash.
          if (testStream.type === DeclarativeStreamType.DeclarativeStream) {
            testStream.schema_loader = {
              type: "InlineSchemaLoader",
              schema: result.inferred_schema,
            } as const;
          }
        } else if (testStreamId.type === "generated_stream") {
          // write the inferred schema to the generated stream's parent dynamic stream
          const dynamicStreamIndex = manifest.dynamic_streams?.findIndex(
            (stream) => stream.name === testStreamId.dynamicStreamName
          );
          if (dynamicStreamIndex !== undefined && dynamicStreamIndex >= 0) {
            setValue(`manifest.dynamic_streams.${dynamicStreamIndex}.stream_template.schema_loader`, schemaLoader, {
              shouldValidate: true,
              shouldTouch: true,
              shouldDirty: true,
            });
          }
        }
      }

      // update the version so that it is clear which CDK version was used to test the connector
      updateYamlCdkVersion(manifest);

      if (testStreamId.type !== "dynamic_stream") {
        updateStreamTestResults(result, testStream);
      }
    }
  );

  const { testingValuesDirty } = useUpdateTestingValuesOnChange();
  const [queuedStreamRead, setQueuedStreamRead] = useState(false);
  const { refetch } = streamRead;
  // trigger a stream read if a stream read is queued and form is in a ready state to be tested
  useEffect(() => {
    if (isResolving || testingValuesDirty || yamlIsDirty || !queuedStreamRead) {
      return;
    }

    if (resolveError) {
      setQueuedStreamRead(false);
      return;
    }

    setQueuedStreamRead(false);
    refetch();
  }, [isResolving, queuedStreamRead, refetch, resolveError, testingValuesDirty, yamlIsDirty]);

  const queueStreamRead = useCallback(() => {
    setQueuedStreamRead(true);
  }, []);

  const cancel = useCancelBuilderProjectStreamRead(projectId, streamName);
  const cancelStreamRead = useCallback(() => {
    // Cancel the query using React Query's remove method
    cancel();
    // Also ensure queuedStreamRead is set to false to prevent auto-refetching
    setQueuedStreamRead(false);
  }, [cancel]);

  const schemaWarnings = useSchemaWarnings(streamRead, testStreamId.index, streamName);

  const ctx = {
    streamRead,
    testReadLimits,
    schemaWarnings,
    testState,
    setTestState,
    queuedStreamRead,
    queueStreamRead,
    cancelStreamRead,
    testStreamRequestType:
      testStream?.type === DeclarativeStreamType.DeclarativeStream &&
      testStream?.retriever?.type === AsyncRetrieverType.AsyncRetriever
        ? ("async" as const)
        : ("sync" as const),
    generatedStreamsLimits,
    generateStreams: { ...fullResolveManifest, refetch: doGenerateStreams } as UseQueryResult<
      ConnectorBuilderProjectFullResolveResponse,
      unknown
    >,
  };

  return <ConnectorBuilderTestReadContext.Provider value={ctx}>{children}</ConnectorBuilderTestReadContext.Provider>;
};

export function useSchemaWarnings(
  streamRead: UseQueryResult<StreamRead, unknown>,
  streamNumber: number,
  streamName: string
) {
  const stream = useBuilderWatch(`manifest.streams.${streamNumber}`);
  const schema =
    stream?.type === DeclarativeStreamType.DeclarativeStream
      ? !Array.isArray(stream.schema_loader) && stream.schema_loader?.type === InlineSchemaLoaderType.InlineSchemaLoader
        ? stream.schema_loader.schema
        : undefined
      : undefined;

  const formattedDetectedSchema = useMemo(() => {
    const inferredSchema = streamRead.data?.inferred_schema;
    if (!inferredSchema) {
      return undefined;
    }
    inferredSchema.additionalProperties = true;
    return formatJson(inferredSchema, true);
  }, [streamRead.data?.inferred_schema]);

  const formattedDeclaredSchema = useMemo(() => {
    if (!schema) {
      return undefined;
    }
    try {
      return formatJson(schema, true);
    } catch {}
    return undefined;
  }, [schema]);

  const [incompatibleSchemaErrors, setIncompatibleSchemaErrors] = useState<string[] | undefined>(undefined);

  useEffect(() => {
    worker.onmessage = (event: MessageEvent<OutgoingData>) => {
      if (event.data.streamName === streamName && schema) {
        setIncompatibleSchemaErrors(event.data.incompatibleSchemaErrors);
      }
    };
  }, [schema, streamName]);

  useEffect(() => {
    const records = streamRead.data?.slices.flatMap((slice) => slice.pages.flatMap((page) => page.records)) || [];
    if (!schema || records.length === 0) {
      setIncompatibleSchemaErrors(undefined);
      return;
    }
    const request: IncomingData = { schema, records, streamName };
    worker.postMessage(request);
  }, [streamRead.data?.slices, schema, streamName]);
  return {
    schemaDifferences: Boolean(
      (formattedDetectedSchema && formattedDeclaredSchema !== formattedDetectedSchema) || incompatibleSchemaErrors
    ),
    incompatibleSchemaErrors,
  };
}

export const useConnectorBuilderTestRead = (): TestReadContext => {
  const connectorBuilderState = useContext(ConnectorBuilderTestReadContext);
  if (!connectorBuilderState) {
    throw new Error("useConnectorBuilderTestRead must be used within a ConnectorBuilderTestReadProvider.");
  }

  return connectorBuilderState;
};

export const useConnectorBuilderFormState = () => {
  const connectorBuilderState = useContext(ConnectorBuilderFormStateContext);
  if (!connectorBuilderState) {
    throw new Error("useConnectorBuilderFormState must be used within a ConnectorBuilderFormStateProvider.");
  }

  return connectorBuilderState;
};

export const useSelectedPageAndSlice = () => {
  const testStreamId = useBuilderWatch("testStreamId");
  const selectedStreamName = useStreamName(testStreamId) ?? "";

  const [streamToSelectedSlice, setStreamToSelectedSlice] = useState({ [selectedStreamName]: 0 });
  const setSelectedSlice = (sliceIndex: number) => {
    setStreamToSelectedSlice((prev) => {
      return { ...prev, [selectedStreamName]: sliceIndex };
    });
    setStreamToSelectedPage((prev) => {
      return { ...prev, [selectedStreamName]: 0 };
    });
  };
  const selectedSlice = streamToSelectedSlice[selectedStreamName] ?? 0;

  const [streamToSelectedPage, setStreamToSelectedPage] = useState({ [selectedStreamName]: 0 });
  const setSelectedPage = (pageIndex: number) => {
    setStreamToSelectedPage((prev) => {
      return { ...prev, [selectedStreamName]: pageIndex };
    });
  };
  const selectedPage = streamToSelectedPage[selectedStreamName] ?? 0;

  return { selectedSlice, selectedPage, setSelectedSlice, setSelectedPage };
};

// check whether paths are equal, normalizing [] and . notation
function arePathsEqual(path1: string, path2: string) {
  return isEqual(toPath(path1), toPath(path2));
}

export const ConnectorBuilderFormManagementStateProvider: React.FC<React.PropsWithChildren<unknown>> = ({
  children,
}) => {
  const [isTestingValuesInputOpen, setTestingValuesInputOpen] = useState(false);
  const [isTestReadSettingsOpen, setTestReadSettingsOpen] = useState(false);
  const [scrollToField, setScrollToField] = useState<string | undefined>(undefined);
  const [newUserInputContext, setNewUserInputContext] = useState<NewUserInputContext | undefined>(undefined);

  const handleScrollToField = useCallback(
    (ref: React.RefObject<HTMLDivElement>, path: string) => {
      if (ref.current && scrollToField && arePathsEqual(path, scrollToField)) {
        ref.current.scrollIntoView({ block: "center" });
        setScrollToField(undefined);
      }
    },
    [scrollToField]
  );

  const ctx = useMemo(
    () => ({
      isTestingValuesInputOpen,
      setTestingValuesInputOpen,
      isTestReadSettingsOpen,
      setTestReadSettingsOpen,
      handleScrollToField,
      setScrollToField,
      newUserInputContext,
      setNewUserInputContext,
    }),
    [isTestingValuesInputOpen, isTestReadSettingsOpen, handleScrollToField, newUserInputContext]
  );

  return (
    <ConnectorBuilderFormManagementStateContext.Provider value={ctx}>
      {children}
    </ConnectorBuilderFormManagementStateContext.Provider>
  );
};

export const useConnectorBuilderFormManagementState = (): FormManagementStateContext => {
  const connectorBuilderState = useContext(ConnectorBuilderFormManagementStateContext);
  if (!connectorBuilderState) {
    throw new Error(
      "useConnectorBuilderFormManagementState must be used within a ConnectorBuilderFormManagementStateProvider."
    );
  }

  return connectorBuilderState;
};
