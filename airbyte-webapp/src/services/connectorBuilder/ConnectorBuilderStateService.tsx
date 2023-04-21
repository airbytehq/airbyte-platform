import { FormikContextType } from "formik";
import { Transition } from "history";
import { dump } from "js-yaml";
import isEqual from "lodash/isEqual";
import React, { useCallback, useContext, useEffect, useMemo, useRef, useState } from "react";
import { useIntl } from "react-intl";
import { UseQueryResult } from "react-query";
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

import { jsonSchemaToFormBlock } from "core/form/schemaToFormBlock";
import { FormGroupItem } from "core/form/types";
import {
  StreamRead,
  StreamReadRequestBodyConfig,
  StreamsListReadStreamsItem,
} from "core/request/ConnectorBuilderClient";
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

interface TestStateContext {
  streams: StreamsListReadStreamsItem[];
  streamListErrorMessage: string | undefined;
  testInputJson: StreamReadRequestBodyConfig;
  setTestInputJson: (value: StreamReadRequestBodyConfig | undefined) => void;
  setTestStreamIndex: (streamIndex: number) => void;
  testStreamIndex: number;
  streamRead: UseQueryResult<StreamRead, unknown>;
  isFetchingStreamList: boolean;
}

interface FormManagementStateContext {
  isTestInputOpen: boolean;
  setTestInputOpen: (open: boolean) => void;
}

export const ConnectorBuilderFormStateContext = React.createContext<FormStateContext | null>(null);
export const ConnectorBuilderTestStateContext = React.createContext<TestStateContext | null>(null);
export const ConnectorBuilderFormManagementStateContext = React.createContext<FormManagementStateContext | null>(null);
export const ConnectorBuilderMainFormikContext = React.createContext<FormikContextType<BuilderFormValues> | null>(null);

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
    if (lastValidJsonManifest.streams.length > 0) {
      newProject.manifest = lastValidJsonManifest;
    }
    await updateProject(newProject);
    setPersistedState(newProject);
  }, [builderFormValues.global.connectorName, lastValidJsonManifest, updateProject]);

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

function useTestInputDefaultValues(testInputJson: StreamReadRequestBodyConfig | undefined, spec?: Spec) {
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

export const ConnectorBuilderTestStateProvider: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => {
  const { formatMessage } = useIntl();
  const { lastValidJsonManifest, selectedView, projectId, editorView, builderFormValues } =
    useConnectorBuilderFormState();

  const manifest = lastValidJsonManifest ?? DEFAULT_JSON_MANIFEST_VALUES;

  // config
  const [testInputJson, setTestInputJson] = useState<StreamReadRequestBodyConfig | undefined>();

  const testInputWithDefaults = useTestInputDefaultValues(testInputJson, manifest.spec);

  // streams
  const {
    data: streamListRead,
    isError: isStreamListError,
    error: streamListError,
    isFetching: isFetchingStreamList,
  } = useListStreams({ manifest, config: testInputWithDefaults });
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

  const streamRead = useReadStream(projectId, {
    manifest,
    stream: streamName,
    config: testInputWithDefaults,
  });

  const ctx = {
    streams,
    streamListErrorMessage,
    testInputJson: testInputWithDefaults,
    setTestInputJson,
    testStreamIndex,
    setTestStreamIndex,
    streamRead,
    isFetchingStreamList,
  };

  return <ConnectorBuilderTestStateContext.Provider value={ctx}>{children}</ConnectorBuilderTestStateContext.Provider>;
};

export const useConnectorBuilderTestState = (): TestStateContext => {
  const connectorBuilderState = useContext(ConnectorBuilderTestStateContext);
  if (!connectorBuilderState) {
    throw new Error("useConnectorBuilderTestStae must be used within a ConnectorBuilderTestStateProvider.");
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
  const { streams, testStreamIndex } = useConnectorBuilderTestState();

  const selectedStreamName = streams[testStreamIndex].name;

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

  const ctx = useMemo(
    () => ({
      isTestInputOpen,
      setTestInputOpen,
    }),
    [isTestInputOpen]
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
