import classnames from "classnames";
import React, { useMemo } from "react";
import { DefaultValues, useFormContext } from "react-hook-form";

import { Builder } from "components/connectorBuilder/Builder/Builder";
import { DEFAULT_JSON_MANIFEST_VALUES_WITH_STREAM } from "components/connectorBuilder/constants";
import { MenuBar } from "components/connectorBuilder/MenuBar";
import { StreamTestingPanel } from "components/connectorBuilder/StreamTestingPanel";
import { BuilderState } from "components/connectorBuilder/types";
import { useBuilderWatch } from "components/connectorBuilder/useBuilderWatch";
import { YamlManifestEditor } from "components/connectorBuilder/YamlEditor";
import { SchemaForm } from "components/forms/SchemaForm/SchemaForm";
import { HeadTitle } from "components/HeadTitle";
import { FlexContainer } from "components/ui/Flex";
import { ResizablePanels } from "components/ui/ResizablePanels";

import {
  ConnectorBuilderResolveProvider,
  useConnectorBuilderResolve,
} from "core/services/connectorBuilder/ConnectorBuilderResolveContext";
import {
  ConnectorBuilderSchemaProvider,
  useConnectorBuilderSchema,
} from "core/services/connectorBuilder/ConnectorBuilderSchemaContext";
import { useExperiment } from "hooks/services/Experiment";
import {
  ConnectorBuilderLocalStorageProvider,
  useConnectorBuilderLocalStorage,
} from "services/connectorBuilder/ConnectorBuilderLocalStorageService";
import {
  ConnectorBuilderTestReadProvider,
  ConnectorBuilderFormStateProvider,
  ConnectorBuilderFormManagementStateProvider,
  ConnectorBuilderMainRHFContext,
} from "services/connectorBuilder/ConnectorBuilderStateService";

import styles from "./ConnectorBuilderEditPage.module.scss";

const ConnectorBuilderEditPageInner: React.FC = React.memo(() => {
  const {
    projectId,
    builderProject: {
      builderProject: { name, componentsFileContent },
      declarativeManifest,
      testingValues: initialTestingValues,
    },
    initialYaml,
    initialResolvedManifest,
  } = useConnectorBuilderResolve();
  const { getStoredMode } = useConnectorBuilderLocalStorage();
  const areDynamicStreamsEnabled = useExperiment("connectorBuilder.dynamicStreams");

  const dynamicStreams = declarativeManifest?.manifest?.dynamic_streams;
  const streams = declarativeManifest?.manifest?.streams;

  const hasDynamicStreams = Array.isArray(dynamicStreams) && dynamicStreams.length > 0;
  const hasStreams = Array.isArray(streams) && streams.length > 0;
  const initialTestStreamId = hasStreams
    ? { type: "stream" as const, index: 0 }
    : areDynamicStreamsEnabled && hasDynamicStreams
    ? { type: "dynamic_stream" as const, index: 0 }
    : { type: "stream" as const, index: 0 };

  const initialView =
    initialTestStreamId.type === "dynamic_stream"
      ? { type: "dynamic_stream" as const, index: 0 }
      : { type: "stream" as const, index: 0 };

  const values: BuilderState = {
    mode: initialResolvedManifest !== null ? getStoredMode(projectId) : "yaml",
    yaml: initialYaml,
    customComponentsCode: componentsFileContent,
    name,
    view: initialView,
    streamTab: "requester" as const,
    testStreamId: initialTestStreamId,
    testingValues: initialTestingValues,
    manifest: initialResolvedManifest ?? DEFAULT_JSON_MANIFEST_VALUES_WITH_STREAM,
    generatedStreams: {},
  };

  const { builderStateSchema } = useConnectorBuilderSchema();

  return (
    <SchemaForm
      schema={builderStateSchema}
      initialValues={values as unknown as DefaultValues<BuilderState>}
      formClassName={styles.form}
      refTargetPath="manifest.definitions.linked"
      disableFormControlsUnderPath="generatedStreams"
      onlyShowErrorIfTouched
    >
      <BaseForm />
    </SchemaForm>
  );
});
ConnectorBuilderEditPageInner.displayName = "ConnectorBuilderEditPageInner";

export const ConnectorBuilderEditPage: React.FC = () => (
  // Handles the state of modals being open, e.g. testing values input and test read settings
  <ConnectorBuilderFormManagementStateProvider>
    {/* Handles local storage flags specific to the builder */}
    <ConnectorBuilderLocalStorageProvider>
      {/* Handles calls to resolve the manifest, so that resolve state is shared across the app */}
      <ConnectorBuilderResolveProvider>
        {/* Contains the schema for the Builder form, so that it can be updated when user inputs are modified */}
        <ConnectorBuilderSchemaProvider>
          <ConnectorBuilderEditPageInner />
        </ConnectorBuilderSchemaProvider>
      </ConnectorBuilderResolveProvider>
    </ConnectorBuilderLocalStorageProvider>
  </ConnectorBuilderFormManagementStateProvider>
);

const BaseForm = () => {
  const methods = useFormContext<BuilderState>();
  return (
    <ConnectorBuilderMainRHFContext.Provider value={methods}>
      {/* The main state context for the Builder */}
      <ConnectorBuilderFormStateProvider>
        {/* Handles the state of the test reads for streams */}
        <ConnectorBuilderTestReadProvider>
          <HeadTitle titles={[{ id: "connectorBuilder.title" }]} />
          <FlexContainer direction="column" gap="none" className={styles.container}>
            <MenuBar />
            <Panels />
          </FlexContainer>
        </ConnectorBuilderTestReadProvider>
      </ConnectorBuilderFormStateProvider>
    </ConnectorBuilderMainRHFContext.Provider>
  );
};

const Panels = React.memo(() => {
  const mode = useBuilderWatch("mode");

  return useMemo(
    () => (
      <ResizablePanels
        // key is used to force re-mount of the form when a different state version is loaded so the react-hook-form / YAML editor state is re-initialized with the new values
        className={classnames(styles.panelsContainer, {
          [styles.gradientBg]: mode === "yaml",
          [styles.solidBg]: mode === "ui",
        })}
        panels={[
          {
            children: mode === "yaml" ? <YamlManifestEditor /> : <Builder />,
            className: styles.leftPanel,
            minWidth: 350,
          },
          {
            children: <StreamTestingPanel />,
            className: styles.rightPanel,
            flex: 0.33,
            minWidth: 250,
          },
        ]}
      />
    ),
    [mode]
  );
});
Panels.displayName = "Panels";
