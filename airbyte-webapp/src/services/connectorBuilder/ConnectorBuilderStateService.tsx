import { UseQueryResult } from "@tanstack/react-query";
import { dump } from "js-yaml";
import isEqual from "lodash/isEqual";
import React, { useCallback, useContext, useEffect, useMemo, useRef, useState } from "react";
import { useFormContext, UseFormReturn } from "react-hook-form";
import { useIntl } from "react-intl";
import { useParams } from "react-router-dom";
import { useDebounce } from "react-use";

import { WaitForSavingModal } from "components/connectorBuilder/Builder/WaitForSavingModal";
import { convertToBuilderFormValuesSync } from "components/connectorBuilder/convertManifestToBuilderForm";
import {
  BuilderState,
  DEFAULT_BUILDER_FORM_VALUES,
  DEFAULT_JSON_MANIFEST_VALUES,
  convertToManifest,
  useBuilderWatch,
} from "components/connectorBuilder/types";
import { useManifestToBuilderForm } from "components/connectorBuilder/useManifestToBuilderForm";
import { formatJson } from "components/connectorBuilder/utils";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import {
  BuilderProject,
  BuilderProjectPublishBody,
  BuilderProjectWithManifest,
  NewVersionBody,
  useBuilderProject,
  usePublishBuilderProject,
  useBuilderReadStream,
  useReleaseNewBuilderProjectVersion,
  useUpdateBuilderProject,
  useBuilderResolvedManifest,
  useBuilderResolvedManifestSuspense,
} from "core/api";
import { useIsForeignWorkspace } from "core/api/cloud";
import { SourceDefinitionIdBody } from "core/api/types/AirbyteClient";
import { ConnectorConfig, KnownExceptionInfo, StreamRead } from "core/api/types/ConnectorBuilderClient";
import { ConnectorManifest, DeclarativeComponentSchema, Spec } from "core/api/types/ConnectorManifest";
import { jsonSchemaToFormBlock } from "core/form/schemaToFormBlock";
import { FormGroupItem } from "core/form/types";
import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { FeatureItem, useFeature } from "core/services/features";
import { Blocker, useBlocker } from "core/services/navigation";
import { removeEmptyProperties } from "core/utils/form";
import { useConfirmationModalService } from "hooks/services/ConfirmationModal";
import { setDefaultValues } from "views/Connector/ConnectorForm/useBuildForm";

import { useConnectorBuilderLocalStorage } from "./ConnectorBuilderLocalStorageService";
import { useConnectorBuilderTestInputState } from "./ConnectorBuilderTestInputService";
import { IncomingData, OutgoingData } from "./SchemaWorker";
import SchemaWorker from "./SchemaWorker?worker";

const worker = new SchemaWorker();

export type BuilderView = "global" | "inputs" | number;

export type SavingState = "loading" | "invalid" | "saved" | "error" | "readonly";

interface FormStateContext {
  jsonManifest: DeclarativeComponentSchema;
  yamlEditorIsMounted: boolean;
  yamlIsValid: boolean;
  savingState: SavingState;
  blockedOnInvalidState: boolean;
  projectId: string;
  currentProject: BuilderProject;
  previousManifestDraft: DeclarativeComponentSchema | undefined;
  displayedVersion: number | undefined;
  formValuesValid: boolean;
  setDisplayedVersion: (value: number | undefined, manifest: DeclarativeComponentSchema) => void;
  updateJsonManifest: (jsonValue: ConnectorManifest) => void;
  setYamlIsValid: (value: boolean) => void;
  setYamlEditorIsMounted: (value: boolean) => void;
  triggerUpdate: () => void;
  publishProject: (options: BuilderProjectPublishBody) => Promise<SourceDefinitionIdBody>;
  releaseNewVersion: (options: NewVersionBody) => Promise<void>;
  toggleUI: (newMode: BuilderState["mode"]) => Promise<void>;
  setFormValuesValid: (value: boolean) => void;
}

interface TestReadContext {
  resolvedManifest: ConnectorManifest;
  resolveErrorMessage: string | undefined;
  resolveError: Error | KnownExceptionInfo | null;
  streamRead: UseQueryResult<StreamRead, unknown>;
  isResolving: boolean;
  testInputJson: ConnectorConfig;
  testInputJsonDirty: boolean;
  setTestInputJson: (value: TestReadContext["testInputJson"] | undefined) => void;
  schemaWarnings: {
    schemaDifferences: boolean;
    incompatibleSchemaErrors: string[] | undefined;
  };
}

interface FormManagementStateContext {
  isTestInputOpen: boolean;
  setTestInputOpen: (open: boolean) => void;
  scrollToField: string | undefined;
  setScrollToField: (field: string | undefined) => void;
  stateKey: number;
  setStateKey: React.Dispatch<React.SetStateAction<number>>;
}

export const ConnectorBuilderFormStateContext = React.createContext<FormStateContext | null>(null);
export const ConnectorBuilderTestReadContext = React.createContext<TestReadContext | null>(null);
export const ConnectorBuilderFormManagementStateContext = React.createContext<FormManagementStateContext | null>(null);
export const ConnectorBuilderMainRHFContext = React.createContext<UseFormReturn<BuilderState, unknown> | null>(null);

export const ConnectorBuilderFormStateProvider: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => {
  const restrictAdminInForeignWorkspace = useFeature(FeatureItem.RestrictAdminInForeignWorkspace);
  if (restrictAdminInForeignWorkspace) {
    return <RestrictedConnectorBuilderFormStateProvider>{children}</RestrictedConnectorBuilderFormStateProvider>;
  }
  return (
    <InternalConnectorBuilderFormStateProvider readOnlyMode={false}>
      {children}
    </InternalConnectorBuilderFormStateProvider>
  );
};

export const RestrictedConnectorBuilderFormStateProvider: React.FC<React.PropsWithChildren<unknown>> = ({
  children,
}) => {
  const isForeignWorkspace = useIsForeignWorkspace();
  return (
    <InternalConnectorBuilderFormStateProvider readOnlyMode={isForeignWorkspace}>
      {children}
    </InternalConnectorBuilderFormStateProvider>
  );
};

function convertJsonToYaml(json: object): string {
  return dump(json, {
    noRefs: true,
  });
}

export const InternalConnectorBuilderFormStateProvider: React.FC<
  React.PropsWithChildren<{ readOnlyMode: boolean }>
> = ({ children, readOnlyMode }) => {
  const { projectId, builderProject, updateProject, updateError } = useInitializedBuilderProject();

  const currentProject: BuilderProject = useMemo(
    () => ({
      name: builderProject.builderProject.name,
      version: builderProject.builderProject.activeDeclarativeManifestVersion
        ? builderProject.builderProject.activeDeclarativeManifestVersion
        : "draft",
      id: builderProject.builderProject.builderProjectId,
      hasDraft: builderProject.builderProject.hasDraft,
      sourceDefinitionId: builderProject.builderProject.sourceDefinitionId,
    }),
    [builderProject.builderProject]
  );

  const { setStateKey } = useConnectorBuilderFormManagementState();
  const { setStoredMode } = useConnectorBuilderLocalStorage();
  const { convertToBuilderFormValues } = useManifestToBuilderForm();
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();
  const analyticsService = useAnalyticsService();

  const [displayedVersion, setDisplayedVersion] = useState<number | undefined>(
    builderProject.declarativeManifest?.version
  );
  const [previousManifestDraft, setPreviousManifestDraft] = useState<ConnectorManifest | undefined>(undefined);
  const [jsonManifest, setJsonManifest] = useState<ConnectorManifest>(
    (builderProject.declarativeManifest?.manifest as DeclarativeComponentSchema) ?? DEFAULT_JSON_MANIFEST_VALUES
  );
  const [yamlIsValid, setYamlIsValid] = useState(true);
  const [yamlEditorIsMounted, setYamlEditorIsMounted] = useState(true);
  const [formValuesValid, setFormValuesValid] = useState(true);

  const { setValue, getValues } = useFormContext();
  const mode = useBuilderWatch("mode");
  const name = useBuilderWatch("name");

  useEffect(() => {
    if (name !== currentProject.name) {
      setPreviousManifestDraft(undefined);
      setDisplayedVersion(undefined);
    }
  }, [currentProject.name, name]);

  // use ref so that updateJsonManifest is not recreated on every change to jsonManifest
  const jsonManifestRef = useRef(jsonManifest);
  jsonManifestRef.current = jsonManifest;
  const updateJsonManifest = useCallback((newManifest: ConnectorManifest) => {
    // ensures that undefined values don't cause an unnecessary save
    const cleanedJsonManifest = removeEmptyProperties(newManifest);
    if (!isEqual(cleanedJsonManifest, jsonManifestRef.current)) {
      setJsonManifest(cleanedJsonManifest);
      setPreviousManifestDraft(undefined);
      setDisplayedVersion(undefined);
    }
  }, []);

  useEffect(() => {
    setStoredMode(mode);
  }, [mode, setStoredMode]);

  const toggleUI = useCallback(
    async (newMode: BuilderState["mode"]) => {
      if (newMode === "yaml") {
        setValue(
          "yaml",
          dump(jsonManifest, {
            noRefs: true,
          })
        );
        setYamlIsValid(true);
        setValue("mode", "yaml");
      } else {
        try {
          if (jsonManifest === DEFAULT_JSON_MANIFEST_VALUES) {
            setValue("mode", "ui");
            return;
          }
          const convertedFormValues = await convertToBuilderFormValues(jsonManifest, projectId);
          const convertedManifest = removeEmptyProperties(convertToManifest(convertedFormValues));
          // set jsonManifest first so that a save isn't triggered
          setJsonManifest(convertedManifest);
          setPersistedState({ name: currentProject.name, manifest: convertedManifest });
          setValue("formValues", convertedFormValues, { shouldValidate: true });
          setValue("mode", "ui");
        } catch (e) {
          openConfirmationModal({
            text: "connectorBuilder.toggleModal.text",
            textValues: { error: e.message as string },
            title: "connectorBuilder.toggleModal.title",
            submitButtonText: "connectorBuilder.toggleModal.submitButton",
            onSubmit: () => {
              setValue("mode", "ui");
              closeConfirmationModal();
              analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.DISCARD_YAML_CHANGES, {
                actionDescription: "YAML changes were discarded due to failure when converting from YAML to UI",
              });
            },
          });
          analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.YAML_TO_UI_CONVERSION_FAILURE, {
            actionDescription: "Failure occured when converting from YAML to UI",
            error_message: e.message,
          });
        }
      }
    },
    [
      analyticsService,
      closeConfirmationModal,
      convertToBuilderFormValues,
      currentProject.name,
      jsonManifest,
      openConfirmationModal,
      projectId,
      setValue,
    ]
  );

  const [persistedState, setPersistedState] = useState<BuilderProjectWithManifest>(() => ({
    manifest: jsonManifest,
    name: builderProject.builderProject.name,
  }));

  const setToVersion = useCallback(
    (version: number | undefined, manifest: DeclarativeComponentSchema) => {
      const updateManifestState = (manifestToProcess: DeclarativeComponentSchema) => {
        const cleanedManifest = removeEmptyProperties(manifestToProcess);
        if (version === undefined) {
          // set persisted state to the current state so that the draft is not saved when switching back to the staged draft
          setPersistedState({ name: currentProject.name, manifest: cleanedManifest });
        }
        // set json manifest first so that a save isn't triggered
        setJsonManifest(cleanedManifest);
        setValue("yaml", convertJsonToYaml(cleanedManifest));
      };

      const view = getValues("view");
      if (typeof view === "number" && manifest.streams.length <= view) {
        // switch back to global view if the selected stream does not exist anymore
        setValue("view", "global");
      }

      if (displayedVersion === undefined && version !== undefined) {
        setPreviousManifestDraft(jsonManifest);
      } else if (version === undefined) {
        setPreviousManifestDraft(undefined);
      }

      const mode = getValues("mode");
      if (mode === "ui") {
        try {
          const formValues = convertToBuilderFormValuesSync(manifest);
          updateManifestState(convertToManifest(formValues));
          setValue("formValues", formValues);
        } catch (e) {
          updateManifestState(manifest);
          setValue("mode", "yaml");
        }
      } else {
        updateManifestState(manifest);
      }

      setDisplayedVersion(version);
      setStateKey((key) => key + 1);
    },
    [currentProject.name, displayedVersion, getValues, jsonManifest, setStateKey, setValue]
  );

  const { mutateAsync: sendPublishRequest } = usePublishBuilderProject();
  const { mutateAsync: sendNewVersionRequest } = useReleaseNewBuilderProjectVersion();

  const publishProject = useCallback(
    async (options: BuilderProjectPublishBody) => {
      const result = await sendPublishRequest(options);
      setDisplayedVersion(1);
      return result;
    },
    [sendPublishRequest]
  );

  const releaseNewVersion = useCallback(
    async (options: NewVersionBody) => {
      await sendNewVersionRequest(options);
      setDisplayedVersion(options.version);
    },
    [sendNewVersionRequest]
  );

  const savingState = getSavingState(
    jsonManifest,
    yamlIsValid,
    formValuesValid,
    mode,
    name,
    persistedState,
    displayedVersion,
    updateError,
    readOnlyMode
  );

  const modeRef = useRef(mode);
  modeRef.current = mode;
  const triggerUpdate = useCallback(async () => {
    if (readOnlyMode) {
      // do not save the project if the user is not a member of the workspace to allow testing with connectors without changing them
      return;
    }
    if (!name) {
      // do not save the project as long as the name is not set
      return;
    }
    // do not save invalid ui-based manifest (e.g. no streams), but always save yaml-based manifest
    if (modeRef.current === "ui" && !formValuesValid) {
      return;
    }
    const newProject: BuilderProjectWithManifest = { name, manifest: jsonManifest };
    await updateProject(newProject);
    setPersistedState(newProject);
  }, [readOnlyMode, name, formValuesValid, jsonManifest, updateProject]);

  useDebounce(
    () => {
      if (displayedVersion) {
        // do not save already released versions as draft
        return;
      }
      if (persistedState.manifest === jsonManifest && persistedState.name === name) {
        // first run of the hook, no need to update
        return;
      }
      triggerUpdate();
    },
    2000,
    [triggerUpdate, name, jsonManifest]
  );

  const { pendingBlocker, blockedOnInvalidState } = useBlockOnSavingState(savingState);

  const ctx: FormStateContext = {
    jsonManifest,
    yamlEditorIsMounted,
    yamlIsValid,
    savingState,
    blockedOnInvalidState,
    projectId,
    currentProject,
    previousManifestDraft,
    displayedVersion,
    formValuesValid,
    setDisplayedVersion: setToVersion,
    updateJsonManifest,
    setYamlIsValid,
    setYamlEditorIsMounted,
    triggerUpdate,
    publishProject,
    releaseNewVersion,
    toggleUI,
    setFormValuesValid,
  };

  return (
    <ConnectorBuilderFormStateContext.Provider value={ctx}>
      {pendingBlocker && <WaitForSavingModal pendingBlocker={pendingBlocker} />}
      {children}
    </ConnectorBuilderFormStateContext.Provider>
  );
};

const EMPTY_SCHEMA = {};

function useTestInputDefaultValues(testInputJson: ConnectorConfig | undefined, spec?: Spec) {
  const currentSpec = useRef<Spec | undefined>(undefined);
  return useMemo(() => {
    if (testInputJson) {
      if (!spec) {
        // don't have a spec, keep the current input
        return testInputJson;
      }
      if (isEqual(currentSpec.current, spec)) {
        // spec is the same as before, keep existing input
        return testInputJson;
      }
    }
    // spec changed, set default values
    currentSpec.current = spec;
    const testInputToUpdate = testInputJson || {};
    try {
      const jsonSchema = spec && spec.connection_specification ? spec.connection_specification : EMPTY_SCHEMA;
      const formFields = jsonSchemaToFormBlock(jsonSchema);
      setDefaultValues(formFields as FormGroupItem, testInputToUpdate, { respectExistingValues: true });
    } catch {
      // spec is user supplied so it might not be valid - prevent crashing the application by just skipping trying to set default values
    }
    return testInputToUpdate;
  }, [spec, testInputJson]);
}

export function useInitializedBuilderProject() {
  const { projectId } = useParams<{
    projectId: string;
  }>();
  if (!projectId) {
    throw new Error("Could not find project id in path");
  }
  const builderProject = useBuilderProject(projectId);
  const { mutateAsync: updateProject, error: updateError } = useUpdateBuilderProject(projectId);
  const resolvedManifest = useBuilderResolvedManifestSuspense(builderProject.declarativeManifest?.manifest, projectId);
  const [initialFormValues, failedInitialFormValueConversion, initialYaml] = useMemo(() => {
    if (!resolvedManifest) {
      // could not resolve manifest, use default form values
      return [
        DEFAULT_BUILDER_FORM_VALUES,
        true,
        convertJsonToYaml(builderProject.declarativeManifest?.manifest ?? DEFAULT_JSON_MANIFEST_VALUES),
      ];
    }
    try {
      return [convertToBuilderFormValuesSync(resolvedManifest), false, convertJsonToYaml(resolvedManifest)];
    } catch (e) {
      // could not convert to form values, use default form values
      return [DEFAULT_BUILDER_FORM_VALUES, true, convertJsonToYaml(resolvedManifest)];
    }
  }, [builderProject.declarativeManifest?.manifest, resolvedManifest]);

  return {
    projectId,
    builderProject,
    updateProject,
    updateError,
    initialFormValues,
    failedInitialFormValueConversion,
    initialYaml,
  };
}

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
          onClose: () => {
            setBlockedOnInvalidState(false);
          },
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
  currentJsonManifest: ConnectorManifest,
  yamlIsValid: boolean,
  formValuesValid: boolean,
  mode: BuilderState["mode"],
  name: string | undefined,
  persistedState: { name: string; manifest?: DeclarativeComponentSchema },
  displayedVersion: number | undefined,
  updateError: Error | null,
  readOnlyMode: boolean
): SavingState {
  if (updateError) {
    return "error";
  }
  if (name === undefined) {
    return "invalid";
  }
  if (mode === "ui" && !formValuesValid) {
    return "invalid";
  }
  if (mode === "yaml" && !yamlIsValid) {
    return "invalid";
  }
  if (readOnlyMode) {
    return "readonly";
  }
  const currentStateIsPersistedState = persistedState.manifest === currentJsonManifest && persistedState.name === name;

  if (currentStateIsPersistedState || displayedVersion !== undefined) {
    return "saved";
  }

  return "loading";
}

export const ConnectorBuilderTestReadProvider: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => {
  const { formatMessage } = useIntl();
  const workspaceId = useCurrentWorkspaceId();
  const { jsonManifest, projectId } = useConnectorBuilderFormState();
  const { setValue } = useFormContext();
  const mode = useBuilderWatch("mode");
  const view = useBuilderWatch("view");
  const testStreamIndex = useBuilderWatch("testStreamIndex");

  const manifest = jsonManifest ?? DEFAULT_JSON_MANIFEST_VALUES;

  // config
  const { testInputJson, setTestInputJson } = useConnectorBuilderTestInputState();

  const testInputWithDefaults = useTestInputDefaultValues(testInputJson, manifest.spec);

  // streams
  const {
    data,
    isError: isResolveError,
    error: resolveError,
    isFetching: isResolving,
  } = useBuilderResolvedManifest(
    {
      manifest,
      workspace_id: workspaceId,
      project_id: projectId,
      form_generated_manifest: mode === "ui",
    },
    // don't need to resolve manifest in UI mode since it doesn't use $refs or $parameters
    mode === "yaml"
  );
  const unknownErrorMessage = formatMessage({ id: "connectorBuilder.unknownError" });
  const resolveErrorMessage = isResolveError
    ? resolveError instanceof Error
      ? resolveError.message || unknownErrorMessage
      : unknownErrorMessage
    : undefined;

  useEffect(() => {
    if (typeof view === "number") {
      setValue("testStreamIndex", view);
    }
  }, [setValue, view]);

  const resolvedManifest =
    mode === "ui" ? manifest : ((data?.manifest ?? DEFAULT_JSON_MANIFEST_VALUES) as ConnectorManifest);
  const testStream = resolvedManifest.streams[testStreamIndex];
  const filteredManifest = {
    ...resolvedManifest,
    streams: [testStream],
  };
  const streamName = testStream?.name ?? "";
  const streamRead = useBuilderReadStream(
    projectId,
    {
      manifest: filteredManifest,
      stream: streamName,
      config: testInputWithDefaults,
      record_limit: 1000,
      workspace_id: workspaceId,
      project_id: projectId,
      form_generated_manifest: mode === "ui",
    },
    (data) => {
      if (data.latest_config_update) {
        setTestInputJson(data.latest_config_update);
      }
    }
  );

  const schemaWarnings = useSchemaWarnings(streamRead, testStreamIndex, streamName);

  const ctx = {
    resolvedManifest,
    resolveErrorMessage,
    resolveError,
    streamRead,
    isResolving,
    testInputJson: testInputWithDefaults,
    testInputJsonDirty: Boolean(testInputJson),
    setTestInputJson,
    schemaWarnings,
  };

  return <ConnectorBuilderTestReadContext.Provider value={ctx}>{children}</ConnectorBuilderTestReadContext.Provider>;
};

export function useSchemaWarnings(
  streamRead: UseQueryResult<StreamRead, unknown>,
  streamNumber: number,
  streamName: string
) {
  const streams = useBuilderWatch("formValues.streams");
  const schema = streams[streamNumber]?.schema;

  const formattedDetectedSchema = useMemo(
    () => streamRead.data?.inferred_schema && formatJson(streamRead.data?.inferred_schema, true),
    [streamRead.data?.inferred_schema]
  );

  const formattedDeclaredSchema = useMemo(() => {
    if (!schema) {
      return undefined;
    }
    try {
      return formatJson(JSON.parse(schema), true);
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

export const useConnectorBuilderFormState = (): FormStateContext => {
  const connectorBuilderState = useContext(ConnectorBuilderFormStateContext);
  if (!connectorBuilderState) {
    throw new Error("useConnectorBuilderFormState must be used within a ConnectorBuilderFormStateProvider.");
  }

  return connectorBuilderState;
};

export const useSelectedPageAndSlice = () => {
  const {
    resolvedManifest: { streams },
  } = useConnectorBuilderTestRead();
  const testStreamIndex = useBuilderWatch("testStreamIndex");

  const selectedStreamName = streams[testStreamIndex]?.name ?? "";

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

export const ConnectorBuilderFormManagementStateProvider: React.FC<React.PropsWithChildren<unknown>> = ({
  children,
}) => {
  const [isTestInputOpen, setTestInputOpen] = useState(false);
  const [scrollToField, setScrollToField] = useState<string | undefined>(undefined);
  const [stateKey, setStateKey] = useState(0);

  const ctx = useMemo(
    () => ({
      isTestInputOpen,
      setTestInputOpen,
      scrollToField,
      setScrollToField,
      stateKey,
      setStateKey,
    }),
    [isTestInputOpen, scrollToField, stateKey]
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
