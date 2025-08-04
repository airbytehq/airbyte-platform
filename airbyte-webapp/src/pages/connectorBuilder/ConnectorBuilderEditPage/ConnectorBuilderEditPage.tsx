import classnames from "classnames";
import React, { useMemo } from "react";
import { DefaultValues, useFormContext } from "react-hook-form";

import { Builder } from "components/connectorBuilder/Builder/Builder";
import {
  RequestBodyGraphQL,
  RequestOptionFieldPath,
  RequestOptionFieldName,
  RequestOptionInjectSelector,
  DeclarativeOAuthWithClientId,
  GrantTypeSelector,
  JinjaBuilderField,
} from "components/connectorBuilder/Builder/overrides";
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
      testingValues: initialTestingValues,
    },
    initialYaml,
    initialResolvedManifest,
  } = useConnectorBuilderResolve();
  const { getStoredMode } = useConnectorBuilderLocalStorage();
  const areDynamicStreamsEnabled = useExperiment("connectorBuilder.dynamicStreams");

  const dynamicStreams = initialResolvedManifest?.dynamic_streams;
  const streams = initialResolvedManifest?.streams;

  const hasDynamicStreams = Array.isArray(dynamicStreams) && dynamicStreams.length > 0;
  const hasStreams = Array.isArray(streams) && streams.length > 0;
  const initialTestStreamId =
    !hasStreams && areDynamicStreamsEnabled && hasDynamicStreams
      ? { type: "dynamic_stream" as const, index: 0 }
      : { type: "stream" as const, index: 0 };

  const initialView =
    initialTestStreamId.type === "dynamic_stream"
      ? { type: "dynamic_stream" as const, index: 0 }
      : hasStreams
      ? { type: "stream" as const, index: 0 }
      : { type: "global" as const };

  const values: BuilderState = {
    mode: initialResolvedManifest !== null ? getStoredMode(projectId) : "yaml",
    yaml: initialYaml,
    customComponentsCode: componentsFileContent,
    name,
    view: initialView,
    streamTab: "requester" as const,
    testStreamId: initialTestStreamId,
    testingValues: initialTestingValues ?? {},
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
      refBasePath="manifest"
      disableFormControlsUnderPath="generatedStreams"
      onlyShowErrorIfTouched
      overrideByObjectField={{
        RequestBodyGraphQL: {
          value: (path) => <RequestBodyGraphQL path={path} />,
        },
        RequestOption: {
          field_name: () => <RequestOptionFieldName />,
          field_path: (path) => <RequestOptionFieldPath path={path} />,
          inject_into: (path) => <RequestOptionInjectSelector path={path} />,
        },
        OAuthAuthenticator: {
          client_id: (path) => <DeclarativeOAuthWithClientId clientIdPath={path} />,
          grant_type: (path) => <GrantTypeSelector path={path} />,
        },
      }}
      overrideByFieldSchema={[
        {
          shouldOverride: (schema) => schema.type === "string" && !!schema.interpolation_context,
          renderOverride: (controlProps) => <JinjaBuilderField {...controlProps} />,
        },
      ]}
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
  const testStreamId = useBuilderWatch("testStreamId");

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
            children: <StreamTestingPanel key={JSON.stringify(testStreamId)} />,
            className: styles.rightPanel,
            flex: 0.33,
            minWidth: 250,
          },
        ]}
      />
    ),
    [mode, testStreamId]
  );
});
Panels.displayName = "Panels";
