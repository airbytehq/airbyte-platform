import { UseMutateAsyncFunction, UseQueryResult } from "@tanstack/react-query";
import { dump } from "js-yaml";
import cloneDeep from "lodash/cloneDeep";
import isEqual from "lodash/isEqual";
import merge from "lodash/merge";
import toPath from "lodash/toPath";
import { editor, Position } from "monaco-editor";
import React, { useCallback, useContext, useEffect, useMemo, useRef, useState } from "react";
import { useFormContext, UseFormReturn } from "react-hook-form";
import { useIntl } from "react-intl";
import { useParams } from "react-router-dom";
import { useDebounce } from "react-use";

import { WaitForSavingModal } from "components/connectorBuilder/Builder/WaitForSavingModal";
import { CDK_VERSION } from "components/connectorBuilder/cdk";
import { convertToBuilderFormValuesSync } from "components/connectorBuilder/convertManifestToBuilderForm";
import {
  BuilderState,
  convertToManifest,
  DEFAULT_BUILDER_FORM_VALUES,
  DEFAULT_JSON_MANIFEST_VALUES,
  useBuilderWatch,
} from "components/connectorBuilder/types";
import { useAutoImportSchema } from "components/connectorBuilder/useAutoImportSchema";
import { useUpdateLockedInputs } from "components/connectorBuilder/useLockedInputs";
import { getStreamHash, useStreamTestMetadata } from "components/connectorBuilder/useStreamTestMetadata";
import { UndoRedo, useUndoRedo } from "components/connectorBuilder/useUndoRedo";
import { formatJson, streamNameOrDefault } from "components/connectorBuilder/utils";
import { useNoUiValueModal } from "components/connectorBuilder/YamlEditor/NoUiValueModal";

import { useCurrentWorkspaceId } from "area/workspace/utils";
import {
  BuilderProject,
  BuilderProjectPublishBody,
  BuilderProjectWithManifest,
  convertProjectDetailsReadToBuilderProject,
  HttpError,
  NewVersionBody,
  StreamReadTransformedSlices,
  useBuilderProject,
  useBuilderProjectReadStream,
  useBuilderProjectUpdateTestingValues,
  useBuilderResolvedManifest,
  useBuilderResolvedManifestSuspense,
  useCurrentWorkspace,
  usePublishBuilderProject,
  useReleaseNewBuilderProjectVersion,
  useUpdateBuilderProject,
} from "core/api";
import { useIsForeignWorkspace } from "core/api/cloud";
import {
  ConnectorBuilderProjectTestingValues,
  ConnectorBuilderProjectTestingValuesUpdate,
  SourceDefinitionIdBody,
} from "core/api/types/AirbyteClient";
import { KnownExceptionInfo, StreamRead } from "core/api/types/ConnectorBuilderClient";
import { ConnectorManifest, DeclarativeComponentSchema, Spec } from "core/api/types/ConnectorManifest";
import { jsonSchemaToFormBlock } from "core/form/schemaToFormBlock";
import { FormGroupItem } from "core/form/types";
import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { FeatureItem, useFeature } from "core/services/features";
import { Blocker, useBlocker } from "core/services/navigation";
import { removeEmptyProperties } from "core/utils/form";
import { useIntent } from "core/utils/rbac";
import { useConfirmationModalService } from "hooks/services/ConfirmationModal";
import { setDefaultValues } from "views/Connector/ConnectorForm/useBuildForm";

import { useConnectorBuilderLocalStorage } from "./ConnectorBuilderLocalStorageService";
import { IncomingData, OutgoingData } from "./SchemaWorker";
import SchemaWorker from "./SchemaWorker?worker";

const worker = new SchemaWorker();

export type BuilderView = "global" | "inputs" | number;

export type SavingState = "loading" | "invalid" | "saved" | "error" | "readonly";

export type ConnectorBuilderPermission = "write" | "readOnly" | "adminReadOnly";

export type TestingValuesUpdate = UseMutateAsyncFunction<
  ConnectorBuilderProjectTestingValues,
  Error,
  Omit<ConnectorBuilderProjectTestingValuesUpdate, "builderProjectId" | "workspaceId">,
  unknown
>;

interface FormStateContext {
  jsonManifest: DeclarativeComponentSchema;
  yamlEditorIsMounted: boolean;
  yamlIsValid: boolean;
  savingState: SavingState;
  permission: ConnectorBuilderPermission;
  blockedOnInvalidState: boolean;
  projectId: string;
  currentProject: BuilderProject;
  previousManifestDraft: DeclarativeComponentSchema | undefined;
  displayedVersion: number | undefined;
  formValuesValid: boolean;
  formValuesDirty: boolean;
  resolvedManifest: ConnectorManifest;
  resolveErrorMessage: string | undefined;
  resolveError: HttpError<KnownExceptionInfo> | null;
  isResolving: boolean;
  streamNames: string[];
  undoRedo: UndoRedo;
  setDisplayedVersion: (value: number | undefined, manifest: DeclarativeComponentSchema) => void;
  updateJsonManifest: (jsonValue: ConnectorManifest) => void;
  setYamlIsValid: (value: boolean) => void;
  setYamlEditorIsMounted: (value: boolean) => void;
  triggerUpdate: () => void;
  publishProject: (options: BuilderProjectPublishBody) => Promise<SourceDefinitionIdBody>;
  releaseNewVersion: (options: NewVersionBody) => Promise<void>;
  toggleUI: (newMode: BuilderState["mode"]) => Promise<void>;
  setFormValuesValid: (value: boolean) => void;
  setFormValuesDirty: (value: boolean) => void;
  updateTestingValues: TestingValuesUpdate;
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
}

interface FormManagementStateContext {
  isTestingValuesInputOpen: boolean;
  setTestingValuesInputOpen: (open: boolean) => void;
  isTestReadSettingsOpen: boolean;
  setTestReadSettingsOpen: (open: boolean) => void;
  handleScrollToField: (ref: React.RefObject<HTMLDivElement>, path: string) => void;
  setScrollToField: (field: string | undefined) => void;
  stateKey: number;
  setStateKey: React.Dispatch<React.SetStateAction<number>>;
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
export const ConnectorBuilderMainRHFContext = React.createContext<UseFormReturn<BuilderState, unknown> | null>(null);

export const ConnectorBuilderFormStateProvider: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => {
  const restrictAdminInForeignWorkspace = useFeature(FeatureItem.RestrictAdminInForeignWorkspace);
  const { workspaceId } = useCurrentWorkspace();
  const canUpdateConnector = useIntent("UpdateCustomConnector", { workspaceId });
  const isForeignWorkspace = useIsForeignWorkspace();

  let permission: ConnectorBuilderPermission = "readOnly";
  if (canUpdateConnector) {
    permission = restrictAdminInForeignWorkspace && isForeignWorkspace ? "adminReadOnly" : "write";
  }

  return (
    <InternalConnectorBuilderFormStateProvider permission={permission}>
      {children}
    </InternalConnectorBuilderFormStateProvider>
  );
};

const MANIFEST_KEY_ORDER: Array<keyof ConnectorManifest> = [
  "version",
  "type",
  "description",
  "check",
  "definitions",
  "streams",
  "spec",
  "metadata",
  "schemas",
];
export function convertJsonToYaml(json: ConnectorManifest): string {
  const yamlString = dump(json, {
    noRefs: true,
    quotingType: '"',
    sortKeys: (a: keyof ConnectorManifest, b: keyof ConnectorManifest) => {
      const orderA = MANIFEST_KEY_ORDER.indexOf(a);
      const orderB = MANIFEST_KEY_ORDER.indexOf(b);
      if (orderA === -1 && orderB === -1) {
        return 0;
      }
      if (orderA === -1) {
        return 1;
      }
      if (orderB === -1) {
        return -1;
      }
      return orderA - orderB;
    },
  });

  // add newlines between root-level fields
  return yamlString.replace(/^\S+.*/gm, (match, offset) => {
    return offset > 0 ? `\n${match}` : match;
  });
}

export const InternalConnectorBuilderFormStateProvider: React.FC<
  React.PropsWithChildren<{ permission: ConnectorBuilderPermission }>
> = ({ children, permission }) => {
  const { formatMessage } = useIntl();
  const { projectId, builderProject, updateProject, updateError } = useInitializedBuilderProject();

  const currentProject: BuilderProject = useMemo(
    () => convertProjectDetailsReadToBuilderProject(builderProject.builderProject),
    [builderProject.builderProject]
  );

  const { setStateKey } = useConnectorBuilderFormManagementState();
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
  const [formValuesDirty, setFormValuesDirty] = useState(false);

  const workspaceId = useCurrentWorkspaceId();

  const { setValue, getValues } = useFormContext();
  const mode = useBuilderWatch("mode");
  const name = useBuilderWatch("name");

  const {
    data: resolveData,
    isError: isResolveError,
    error: resolveError,
    isFetching: isResolving,
  } = useBuilderResolvedManifest(
    {
      manifest: jsonManifest,
      workspace_id: workspaceId,
      project_id: projectId,
      form_generated_manifest: mode === "ui",
    },
    // In UI mode, only call resolve if the form is valid, since an invalid form is expected to not resolve
    mode === "yaml" || (mode === "ui" && formValuesValid)
  );

  const unknownErrorMessage = formatMessage({ id: "connectorBuilder.unknownError" });
  const resolveErrorMessage = isResolveError
    ? resolveError instanceof HttpError
      ? resolveError.response?.message || unknownErrorMessage
      : unknownErrorMessage
    : undefined;

  const resolvedManifest = (resolveData?.manifest ?? DEFAULT_JSON_MANIFEST_VALUES) as ConnectorManifest;

  const streams = useBuilderWatch("formValues.streams");
  const streamNames =
    mode === "ui"
      ? streams.map((stream) => stream.name)
      : resolvedManifest.streams?.map((stream) => stream.name ?? "") ?? [];

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

  const formValues = useBuilderWatch("formValues");
  const openNoUiValueModal = useNoUiValueModal();

  const toggleUI = useCallback(
    async (newMode: BuilderState["mode"]) => {
      if (newMode === "yaml") {
        setValue("yaml", convertJsonToYaml(jsonManifest));
        setYamlIsValid(true);
        setValue("mode", "yaml");
      } else {
        const confirmDiscard = (errorMessage: string) => {
          if (
            isEqual(formValues, DEFAULT_BUILDER_FORM_VALUES) &&
            (!jsonManifest.streams || jsonManifest.streams.length > 0)
          ) {
            openNoUiValueModal(errorMessage);
          } else {
            openConfirmationModal({
              text: "connectorBuilder.toggleModal.text.uiValueAvailable",
              textValues: { error: errorMessage },
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
          }
        };
        try {
          if (isEqual(jsonManifest, removeEmptyProperties(DEFAULT_JSON_MANIFEST_VALUES))) {
            setValue("mode", "ui");
            return;
          }
          if (isResolveError) {
            confirmDiscard(resolveErrorMessage ?? "");
            return;
          }
          const convertedFormValues = convertToBuilderFormValuesSync(resolvedManifest);
          const convertedManifest = removeEmptyProperties(convertToManifest(convertedFormValues));
          // set jsonManifest first so that a save isn't triggered
          setJsonManifest(convertedManifest);
          setPersistedState({ name: currentProject.name, manifest: convertedManifest });
          // don't need to explicitly validate here, since automatic form validation will still prevent
          // publishing if there are any form errors
          setValue("formValues", convertedFormValues, { shouldValidate: false });
          setValue("mode", "ui");
        } catch (e) {
          confirmDiscard(e.message);
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
      currentProject.name,
      formValues,
      isResolveError,
      jsonManifest,
      openConfirmationModal,
      openNoUiValueModal,
      resolveErrorMessage,
      resolvedManifest,
      setValue,
    ]
  );

  const updateYamlCdkVersion = useCallback(
    (currentManifest: ConnectorManifest) => {
      if (mode === "yaml") {
        const newManifest = { ...currentManifest, version: CDK_VERSION };
        setValue("yaml", convertJsonToYaml(newManifest));
        return newManifest;
      }
      return currentManifest;
    },
    [mode, setValue]
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
      if (typeof view === "number" && manifest.streams && manifest.streams.length <= view) {
        // switch back to global view if the selected stream does not exist anymore
        setValue("view", "global");
      }

      if (displayedVersion === undefined && version !== undefined) {
        setPreviousManifestDraft(resolvedManifest);
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
    [currentProject.name, displayedVersion, getValues, resolvedManifest, setStateKey, setValue]
  );

  const { mutateAsync: sendPublishRequest } = usePublishBuilderProject();
  const { mutateAsync: sendNewVersionRequest } = useReleaseNewBuilderProjectVersion();

  const publishProject = useCallback(
    async (options: BuilderProjectPublishBody) => {
      // update the version so that the manifest reflects which CDK version was used to build it
      const updatedManifest = updateYamlCdkVersion(jsonManifest);
      const result = await sendPublishRequest({ ...options, manifest: updatedManifest });
      setDisplayedVersion(1);
      return result;
    },
    [jsonManifest, sendPublishRequest, updateYamlCdkVersion]
  );

  const releaseNewVersion = useCallback(
    async (options: NewVersionBody) => {
      // update the version so that the manifest reflects which CDK version was used to build it
      const updatedManifest = updateYamlCdkVersion(jsonManifest);
      await sendNewVersionRequest({ ...options, manifest: updatedManifest });
      setDisplayedVersion(options.version);
    },
    [jsonManifest, sendNewVersionRequest, updateYamlCdkVersion]
  );

  const formAndResolveValid = useMemo(() => formValuesValid && resolveError === null, [formValuesValid, resolveError]);

  const savingState = getSavingState(
    jsonManifest,
    yamlIsValid,
    formAndResolveValid,
    mode,
    name,
    persistedState,
    displayedVersion,
    updateError,
    permission
  );

  const modeRef = useRef(mode);
  modeRef.current = mode;
  const triggerUpdate = useCallback(async () => {
    if (permission !== "write") {
      // do not save the project if the user is not a member of the workspace to allow testing with connectors without changing them
      return;
    }
    if (!name) {
      // do not save the project as long as the name is not set
      return;
    }
    // do not save invalid ui-based manifest (e.g. no streams), but always save yaml-based manifest
    if (modeRef.current === "ui" && !formAndResolveValid) {
      return;
    }
    const newProject: BuilderProjectWithManifest = {
      name,
      manifest: jsonManifest,
      yamlManifest: convertJsonToYaml(jsonManifest),
    };
    await updateProject(newProject);
    setPersistedState(newProject);
  }, [permission, name, formAndResolveValid, jsonManifest, updateProject]);

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

  const { mutateAsync: updateTestingValues } = useBuilderProjectUpdateTestingValues(projectId, (result) =>
    setValue("testingValues", result)
  );

  useUpdateTestingValuesOnSpecChange(jsonManifest.spec, updateTestingValues);

  useUpdateLockedInputs();

  const undoRedo = useUndoRedo();

  const ctx: FormStateContext = {
    jsonManifest,
    yamlEditorIsMounted,
    yamlIsValid,
    savingState,
    permission,
    blockedOnInvalidState,
    projectId,
    currentProject,
    previousManifestDraft,
    displayedVersion,
    formValuesValid,
    formValuesDirty,
    resolvedManifest,
    resolveError,
    resolveErrorMessage,
    isResolving,
    streamNames,
    undoRedo,
    setDisplayedVersion: setToVersion,
    updateJsonManifest,
    setYamlIsValid,
    setYamlEditorIsMounted,
    triggerUpdate,
    publishProject,
    releaseNewVersion,
    toggleUI,
    setFormValuesValid,
    setFormValuesDirty,
    updateTestingValues,
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

const EMPTY_SCHEMA = {};

const useUpdateTestingValuesOnSpecChange = (
  spec: Spec | undefined,
  updateTestingValues: FormStateContext["updateTestingValues"]
) => {
  const testingValues = useBuilderWatch("testingValues");
  const specRef = useRef<Spec | undefined>(spec);

  useEffect(() => {
    if (!isEqual(specRef.current?.connection_specification, spec?.connection_specification)) {
      // clone testingValues because applyTestingValuesDefaults mutates the object
      const testingValuesWithDefaults = applyTestingValuesDefaults(cloneDeep(testingValues), spec);
      if (!isEqual(testingValues, testingValuesWithDefaults)) {
        updateTestingValues({
          spec: spec?.connection_specification ?? {},
          testingValues: testingValuesWithDefaults ?? {},
        });
      }
    }
    specRef.current = spec;
  }, [spec, testingValues, updateTestingValues]);
};

export function applyTestingValuesDefaults(
  testingValues: ConnectorBuilderProjectTestingValues | undefined,
  spec?: Spec
) {
  const testingValuesToUpdate = testingValues || {};
  try {
    const jsonSchema = spec && spec.connection_specification ? spec.connection_specification : EMPTY_SCHEMA;
    const formFields = jsonSchemaToFormBlock(jsonSchema);
    setDefaultValues(formFields as FormGroupItem, testingValuesToUpdate, { respectExistingValues: true });
  } catch {
    // spec is user supplied so it might not be valid - prevent crashing the application by just skipping trying to set default values
  }

  return testingValues === undefined && Object.keys(testingValuesToUpdate).length === 0
    ? undefined
    : testingValuesToUpdate;
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
  const persistedManifest =
    (builderProject.declarativeManifest?.manifest as ConnectorManifest) ?? DEFAULT_JSON_MANIFEST_VALUES;
  const resolvedManifest = useBuilderResolvedManifestSuspense(builderProject.declarativeManifest?.manifest, projectId);
  const [initialFormValues, failedInitialFormValueConversion, initialYaml] = useMemo(() => {
    if (!resolvedManifest) {
      // could not resolve manifest, use default form values
      return [DEFAULT_BUILDER_FORM_VALUES, true, convertJsonToYaml(persistedManifest)];
    }
    setInitialStreamHashes(persistedManifest, resolvedManifest);
    try {
      return [convertToBuilderFormValuesSync(resolvedManifest), false, convertJsonToYaml(persistedManifest)];
    } catch (e) {
      // could not convert to form values, use default form values
      return [DEFAULT_BUILDER_FORM_VALUES, true, convertJsonToYaml(persistedManifest)];
    }
  }, [persistedManifest, resolvedManifest]);

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
    const streamName = streamNameOrDefault(resolvedStream.name, i);
    if (persistedManifest.metadata?.testedStreams?.[streamName]) {
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
  currentJsonManifest: ConnectorManifest,
  yamlIsValid: boolean,
  formAndResolveValid: boolean,
  mode: BuilderState["mode"],
  name: string | undefined,
  persistedState: { name: string; manifest?: DeclarativeComponentSchema },
  displayedVersion: number | undefined,
  updateError: Error | null,
  permission: ConnectorBuilderPermission
): SavingState {
  if (updateError) {
    return "error";
  }
  if (!name) {
    return "invalid";
  }
  if (mode === "ui" && !formAndResolveValid) {
    return "invalid";
  }
  if (mode === "yaml" && !yamlIsValid) {
    return "invalid";
  }
  if (permission !== "write") {
    return "readonly";
  }
  const currentStateIsPersistedState = persistedState.manifest === currentJsonManifest && persistedState.name === name;

  if (currentStateIsPersistedState || displayedVersion !== undefined) {
    return "saved";
  }

  return "loading";
}

export const ConnectorBuilderTestReadProvider: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => {
  const workspaceId = useCurrentWorkspaceId();
  const {
    projectId,
    isResolving,
    resolveError,
    resolvedManifest,
    jsonManifest,
    formValuesDirty,
    updateYamlCdkVersion,
  } = useConnectorBuilderFormState();
  const { setValue } = useFormContext();
  const mode = useBuilderWatch("mode");
  const view = useBuilderWatch("view");
  const testStreamIndex = useBuilderWatch("testStreamIndex");

  useEffect(() => {
    if (typeof view === "number") {
      setValue("testStreamIndex", view);
    }
  }, [setValue, view]);

  const testStream = resolvedManifest.streams?.[testStreamIndex];
  const filteredManifest = {
    ...resolvedManifest,
    streams: [testStream],
  };
  const streamName = testStream?.name ?? "";

  const DEFAULT_PAGE_LIMIT = 5;
  const DEFAULT_SLICE_LIMIT = 5;
  const DEFAULT_RECORD_LIMIT = 1000;

  const [pageLimit, setPageLimit] = useState(DEFAULT_PAGE_LIMIT);
  const [sliceLimit, setSliceLimit] = useState(DEFAULT_SLICE_LIMIT);
  const [recordLimit, setRecordLimit] = useState(DEFAULT_RECORD_LIMIT);
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

  const testStateParsed = testState ? JSON.parse(testState) : undefined;
  const testStateArray = testStateParsed && !Array.isArray(testStateParsed) ? [testStateParsed] : testStateParsed;

  const autoImportSchema = useAutoImportSchema(testStreamIndex);
  const { updateStreamTestResults } = useStreamTestMetadata();

  const streamRead = useBuilderProjectReadStream(
    {
      builderProjectId: projectId,
      manifest: filteredManifest,
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
        // additionalProperties is automatically set to true on the schema when saving it to the manifest,
        // so set it to true on the inferred schema as well to avoid unnecessary diffs
        result.inferred_schema.additionalProperties = true;

        // Set the inferred schema in the form values when autoImportSchema is enabled
        setValue(`formValues.streams.${testStreamIndex}.schema`, formatJson(result.inferred_schema, true), {
          shouldValidate: true,
          shouldTouch: true,
          shouldDirty: true,
        });

        // Set the schema_loader on the test stream to the inferred schema as well, so
        // that it is included in the stream when generating the test result stream hash.

        testStream.schema_loader = {
          type: "InlineSchemaLoader",
          schema: result.inferred_schema,
        } as const;
      }

      // update the version so that it is clear which CDK version was used to test the connector
      updateYamlCdkVersion(jsonManifest);

      updateStreamTestResults(result, testStream, streamName, testStreamIndex);
    }
  );

  const [queuedStreamRead, setQueuedStreamRead] = useState(false);
  const { refetch } = streamRead;
  // trigger a stream read if the a stream read is queued and form is in a ready state to be tested
  useEffect(() => {
    if (isResolving || formValuesDirty || !queuedStreamRead) {
      return;
    }

    if (resolveError) {
      setQueuedStreamRead(false);
      return;
    }

    setQueuedStreamRead(false);
    refetch();
  }, [isResolving, queuedStreamRead, resolveError, refetch, formValuesDirty]);

  const queueStreamRead = useCallback(() => {
    setQueuedStreamRead(true);
  }, []);

  const schemaWarnings = useSchemaWarnings(streamRead, testStreamIndex, streamName);

  const ctx = {
    streamRead,
    testReadLimits,
    schemaWarnings,
    testState,
    setTestState,
    queuedStreamRead,
    queueStreamRead,
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
  const { streamNames } = useConnectorBuilderFormState();
  const testStreamIndex = useBuilderWatch("testStreamIndex");

  const selectedStreamName = streamNameOrDefault(streamNames[testStreamIndex], testStreamIndex);

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
  const [stateKey, setStateKey] = useState(0);
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
      stateKey,
      setStateKey,
      newUserInputContext,
      setNewUserInputContext,
    }),
    [isTestingValuesInputOpen, isTestReadSettingsOpen, handleScrollToField, stateKey, newUserInputContext]
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
