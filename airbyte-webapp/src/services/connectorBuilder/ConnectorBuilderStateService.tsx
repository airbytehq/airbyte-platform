import { UseQueryResult } from "@tanstack/react-query";
import { Transition } from "history";
import { dump } from "js-yaml";
import isEqual from "lodash/isEqual";
import React, { useCallback, useContext, useEffect, useMemo, useRef, useState } from "react";
import { UseFormReturn } from "react-hook-form";
import { useIntl } from "react-intl";
import { useParams } from "react-router-dom";
import { useDebounce, useEffectOnce } from "react-use";

import { WaitForSavingModal } from "components/connectorBuilder/Builder/WaitForSavingModal";
import { convertToBuilderFormValuesSync } from "components/connectorBuilder/convertManifestToBuilderForm";
import {
  BuilderFormValues,
  convertToManifest,
  DEFAULT_BUILDER_FORM_VALUES,
  DEFAULT_JSON_MANIFEST_VALUES,
  EditorView,
} from "components/connectorBuilder/types";
import { formatJson } from "components/connectorBuilder/utils";

import { jsonSchemaToFormBlock } from "core/form/schemaToFormBlock";
import { FormGroupItem } from "core/form/types";
import { ConnectorConfig, StreamRead, StreamsListReadStreamsItem } from "core/request/ConnectorBuilderClient";
import { ConnectorManifest, DeclarativeComponentSchema, Spec } from "core/request/ConnectorManifest";
import { useBlocker } from "hooks/router/useBlocker";
import { useConfirmationModalService } from "hooks/services/ConfirmationModal";
import { setDefaultValues } from "views/Connector/ConnectorForm/useBuildForm";

import { useListStreams, useReadStream, useResolvedManifest } from "./ConnectorBuilderApiService";
import { useConnectorBuilderLocalStorage } from "./ConnectorBuilderLocalStorageService";
import {
  BuilderProject,
  BuilderProjectWithManifest,
  useProject,
  useUpdateProject,
} from "./ConnectorBuilderProjectsService";
import { useConnectorBuilderTestInputState } from "./ConnectorBuilderTestInputService";
import { IncomingData, OutgoingData } from "./SchemaWorker";
import SchemaWorker from "./SchemaWorker?worker";

const worker = new SchemaWorker();

export type BuilderView = "global" | "inputs" | number;

export type SavingState = "loading" | "invalid" | "saved" | "error";

interface FormStateContext {
  builderFormValues: BuilderFormValues;
  formValuesValid: boolean;
  jsonManifest: ConnectorManifest;
  lastValidJsonManifest: DeclarativeComponentSchema | undefined;
  yamlManifest: string;
  yamlEditorIsMounted: boolean;
  yamlIsValid: boolean;
  selectedView: BuilderView;
  editorView: EditorView;
  savingState: SavingState;
  blockedOnInvalidState: boolean;
  projectId: string;
  currentProject: BuilderProject;
  setBuilderFormValues: (values: BuilderFormValues, isInvalid: boolean) => void;
  setJsonManifest: (jsonValue: ConnectorManifest) => void;
  setYamlEditorIsMounted: (value: boolean) => void;
  setYamlIsValid: (value: boolean) => void;
  setSelectedView: (view: BuilderView) => void;
  setEditorView: (editorView: EditorView) => void;
  triggerUpdate: () => void;
}

interface TestReadContext {
  streams: StreamsListReadStreamsItem[];
  streamListErrorMessage: string | undefined;
  setTestStreamIndex: (streamIndex: number) => void;
  testStreamIndex: number;
  streamRead: UseQueryResult<StreamRead, unknown>;
  isFetchingStreamList: boolean;
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
}

export const ConnectorBuilderFormStateContext = React.createContext<FormStateContext | null>(null);
export const ConnectorBuilderTestReadContext = React.createContext<TestReadContext | null>(null);
export const ConnectorBuilderFormManagementStateContext = React.createContext<FormManagementStateContext | null>(null);
export const ConnectorBuilderMainRHFContext = React.createContext<UseFormReturn<BuilderFormValues, unknown> | null>(
  null
);

export const ConnectorBuilderFormStateProvider: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => {
  const { projectId } = useParams<{
    projectId: string;
  }>();
  if (!projectId) {
    throw new Error("Could not find project id in path");
  }
  const { storedEditorView, setStoredEditorView } = useConnectorBuilderLocalStorage();
  const { builderProject, failedInitialFormValueConversion, initialFormValues, updateProject, updateError } =
    useInitializedBuilderProject(projectId);

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

  const [jsonManifest, setJsonManifest] = useState<DeclarativeComponentSchema>(
    (builderProject.declarativeManifest?.manifest as DeclarativeComponentSchema) || DEFAULT_JSON_MANIFEST_VALUES
  );
  const [builderFormValues, setStoredFormValues] = useState<BuilderFormValues>(initialFormValues);

  useEffectOnce(() => {
    if (failedInitialFormValueConversion && storedEditorView === "ui") {
      setStoredEditorView("yaml");
    }
  });

  const lastValidBuilderFormValuesRef = useRef<BuilderFormValues>(builderFormValues);

  const [formValuesValid, setFormValuesValid] = useState(true);

  const setBuilderFormValues = useCallback(
    (values: BuilderFormValues, isValid: boolean) => {
      if (isValid) {
        // update ref first because calling setStoredBuilderFormValues might synchronously kick off a react render cycle.
        lastValidBuilderFormValuesRef.current = values;
      }
      setStoredFormValues(values);
      setFormValuesValid(isValid);
    },
    [setStoredFormValues]
  );

  const derivedJsonManifest = useMemo(
    () => (storedEditorView === "yaml" ? jsonManifest : convertToManifest(builderFormValues)),
    [storedEditorView, builderFormValues, jsonManifest]
  );

  const manifestRef = useRef(derivedJsonManifest);
  manifestRef.current = derivedJsonManifest;

  const setEditorView = useCallback(
    (view: EditorView) => {
      if (view === "yaml") {
        // when switching to yaml, store the currently derived json manifest
        setJsonManifest(manifestRef.current);
      }
      setStoredEditorView(view);
    },
    [setStoredEditorView, setJsonManifest]
  );

  const [yamlIsValid, setYamlIsValid] = useState(true);
  const [yamlEditorIsMounted, setYamlEditorIsMounted] = useState(true);

  const yamlManifest = useMemo(
    () =>
      dump(derivedJsonManifest, {
        noRefs: true,
      }),
    [derivedJsonManifest]
  );

  const lastValidBuilderFormValues = lastValidBuilderFormValuesRef.current;
  /**
   * The json manifest derived from the last valid state of the builder form values.
   * In the yaml view, this is undefined. Can still be invalid in case an invalid state is loaded from localstorage
   */
  const lastValidJsonManifest = useMemo(
    () =>
      storedEditorView !== "ui"
        ? jsonManifest
        : builderFormValues === lastValidBuilderFormValues
        ? derivedJsonManifest
        : convertToManifest(lastValidBuilderFormValues),
    [builderFormValues, storedEditorView, jsonManifest, derivedJsonManifest, lastValidBuilderFormValues]
  );

  const [persistedState, setPersistedState] = useState<BuilderProjectWithManifest>(() => ({
    manifest: lastValidJsonManifest,
    name: builderProject.builderProject.name,
  }));

  const [selectedView, setSelectedView] = useState<BuilderView>("global");

  const savingState = getSavingState(
    storedEditorView,
    yamlIsValid,
    persistedState,
    builderFormValues,
    lastValidJsonManifest,
    formValuesValid,
    updateError
  );

  const triggerUpdate = useCallback(async () => {
    if (!builderFormValues.global.connectorName) {
      // do not save the project as long as the name is not set
      return;
    }
    const newProject: BuilderProjectWithManifest = { name: builderFormValues.global.connectorName };
    // do not save invalid ui-based manifest (e.g. no streams), but always save yaml-based manifest
    if (storedEditorView === "yaml" || lastValidJsonManifest.streams.length > 0) {
      newProject.manifest = lastValidJsonManifest;
    }
    await updateProject(newProject);
    setPersistedState(newProject);
  }, [builderFormValues.global.connectorName, lastValidJsonManifest, storedEditorView, updateProject]);

  useDebounce(
    () => {
      if (
        persistedState.manifest === lastValidJsonManifest &&
        persistedState.name === builderFormValues.global.connectorName
      ) {
        // first run of the hook, no need to update
        return;
      }
      triggerUpdate();
    },
    2500,
    [triggerUpdate, builderFormValues.global.connectorName, lastValidJsonManifest]
  );

  const { pendingTransition, blockedOnInvalidState } = useBlockOnSavingState(savingState);

  const ctx: FormStateContext = {
    builderFormValues,
    formValuesValid,
    jsonManifest: derivedJsonManifest,
    lastValidJsonManifest,
    yamlManifest,
    yamlEditorIsMounted,
    yamlIsValid,
    selectedView,
    editorView: storedEditorView,
    savingState,
    blockedOnInvalidState,
    projectId,
    currentProject,
    setBuilderFormValues,
    setJsonManifest,
    setYamlIsValid,
    setYamlEditorIsMounted,
    setSelectedView,
    setEditorView,
    triggerUpdate,
  };

  return (
    <ConnectorBuilderFormStateContext.Provider value={ctx}>
      {pendingTransition && <WaitForSavingModal pendingTransition={pendingTransition} />}
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

function useInitializedBuilderProject(projectId: string) {
  const builderProject = useProject(projectId);
  const { mutateAsync: updateProject, error: updateError } = useUpdateProject(projectId);
  const resolvedManifest = useResolvedManifest(builderProject.declarativeManifest?.manifest);
  const [initialFormValues, failedInitialFormValueConversion] = useMemo(() => {
    if (!resolvedManifest) {
      // could not resolve manifest, use default form values
      return [getDefaultFormValuesWithName(builderProject.builderProject.name), true];
    }
    try {
      return [convertToBuilderFormValuesSync(resolvedManifest, builderProject.builderProject.name), false];
    } catch (e) {
      // could not convert to form values, use default form values
      return [getDefaultFormValuesWithName(builderProject.builderProject.name), true];
    }
  }, [builderProject.builderProject.name, resolvedManifest]);

  return {
    builderProject,
    updateProject,
    updateError,
    initialFormValues,
    failedInitialFormValueConversion,
  };
}

function useBlockOnSavingState(savingState: SavingState) {
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();
  const [pendingTransition, setPendingTransition] = useState<undefined | Transition>();
  const [blockedOnInvalidState, setBlockedOnInvalidState] = useState(false);
  const blocker = useCallback(
    (tx: Transition) => {
      if (savingState === "invalid" || savingState === "error") {
        setBlockedOnInvalidState(true);
        openConfirmationModal({
          title: "form.discardChanges",
          text: "connectorBuilder.discardChangesConfirmation",
          submitButtonText: "form.discardChanges",
          onSubmit: () => {
            closeConfirmationModal();
            tx.retry();
          },
          onClose: () => {
            setBlockedOnInvalidState(false);
          },
        });
      } else {
        setPendingTransition(tx);
      }
    },
    [closeConfirmationModal, openConfirmationModal, savingState]
  );

  useBlocker(blocker, savingState !== "saved");

  useEffect(() => {
    if (savingState === "saved" && pendingTransition) {
      pendingTransition.retry();
    }
  }, [savingState, pendingTransition]);

  return { pendingTransition, blockedOnInvalidState };
}

function getDefaultFormValuesWithName(name: string) {
  return {
    ...DEFAULT_BUILDER_FORM_VALUES,
    global: { ...DEFAULT_BUILDER_FORM_VALUES.global, connectorName: name },
  };
}

function getSavingState(
  storedEditorView: string,
  yamlIsValid: boolean,
  persistedState: { name: string; manifest?: DeclarativeComponentSchema },
  formValues: BuilderFormValues,
  lastValidJsonManifest: DeclarativeComponentSchema,
  formValuesValid: boolean,
  updateError: Error | null
): SavingState {
  if (updateError) {
    return "error";
  }
  if (storedEditorView === "yaml" && (!yamlIsValid || !formValues.global.connectorName)) {
    return "invalid";
  }
  if (storedEditorView === "ui" && !formValuesValid) {
    return "invalid";
  }
  const currentStateIsPersistedState =
    persistedState.manifest === lastValidJsonManifest && persistedState.name === formValues.global.connectorName;

  if (currentStateIsPersistedState) {
    return "saved";
  }

  return "loading";
}

export const ConnectorBuilderTestReadProvider: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => {
  const { formatMessage } = useIntl();
  const { lastValidJsonManifest, selectedView, projectId, editorView, builderFormValues } =
    useConnectorBuilderFormState();

  const manifest = lastValidJsonManifest ?? DEFAULT_JSON_MANIFEST_VALUES;

  // config
  const { testInputJson, setTestInputJson } = useConnectorBuilderTestInputState();

  const testInputWithDefaults = useTestInputDefaultValues(testInputJson, manifest.spec);

  // streams
  const {
    data: streamListRead,
    isError: isStreamListError,
    error: streamListError,
    isFetching: isFetchingStreamList,
  } = useListStreams(
    { manifest, config: testInputWithDefaults },
    Boolean(editorView === "yaml" || manifest.streams?.length)
  );
  const unknownErrorMessage = formatMessage({ id: "connectorBuilder.unknownError" });
  const streamListErrorMessage = isStreamListError
    ? streamListError instanceof Error
      ? streamListError.message || unknownErrorMessage
      : unknownErrorMessage
    : undefined;
  const streams = useMemo(() => {
    return streamListRead?.streams ?? [];
  }, [streamListRead]);

  const [testStreamIndex, setTestStreamIndex] = useState(0);
  useEffect(() => {
    if (typeof selectedView === "number") {
      setTestStreamIndex(selectedView);
    }
  }, [selectedView]);

  const streamName =
    editorView === "ui" ? builderFormValues.streams[testStreamIndex]?.name : streams[testStreamIndex]?.name;

  const streamRead = useReadStream(
    projectId,
    {
      manifest,
      stream: streamName,
      config: testInputWithDefaults,
      record_limit: 1000,
    },
    (data) => {
      if (data.latest_config_update) {
        setTestInputJson(data.latest_config_update);
      }
    }
  );

  const schemaWarnings = useSchemaWarnings(streamRead, testStreamIndex, streamName);

  const ctx = {
    streams,
    streamListErrorMessage,
    testStreamIndex,
    setTestStreamIndex,
    streamRead,
    isFetchingStreamList,
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
  const { builderFormValues } = useConnectorBuilderFormState();
  const schema = builderFormValues.streams[streamNumber]?.schema;

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
  const { streams, testStreamIndex } = useConnectorBuilderTestRead();

  const selectedStreamName = streams[testStreamIndex]?.name;

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

  const ctx = useMemo(
    () => ({
      isTestInputOpen,
      setTestInputOpen,
      scrollToField,
      setScrollToField,
    }),
    [isTestInputOpen, scrollToField]
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
