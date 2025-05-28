import { yupResolver } from "@hookform/resolvers/yup";
import classnames from "classnames";
import React, { useMemo, useRef } from "react";
import { FormProvider, useForm } from "react-hook-form";
import { AnyObjectSchema } from "yup";

import { Builder } from "components/connectorBuilder/Builder/Builder";
import { MenuBar } from "components/connectorBuilder/MenuBar";
import SchemaFormBuilder from "components/connectorBuilder/SchemaFormBuilder";
import { StreamTestingPanel } from "components/connectorBuilder/StreamTestingPanel";
import { BuilderState } from "components/connectorBuilder/types";
import { useBuilderValidationSchema } from "components/connectorBuilder/useBuilderValidationSchema";
import { useBuilderWatch } from "components/connectorBuilder/useBuilderWatch";
import { YamlManifestEditor } from "components/connectorBuilder/YamlEditor";
import { HeadTitle } from "components/HeadTitle";
import { FlexContainer } from "components/ui/Flex";
import { ResizablePanels } from "components/ui/ResizablePanels";

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
  useInitializedBuilderProject,
  useConnectorBuilderFormManagementState,
} from "services/connectorBuilder/ConnectorBuilderStateService";

import styles from "./ConnectorBuilderEditPage.module.scss";
const ConnectorBuilderEditPageInner: React.FC = React.memo(() => {
  const {
    projectId,
    initialFormValues,
    failedInitialFormValueConversion,
    initialYaml,
    builderProject: {
      builderProject: { name, componentsFileContent },
      declarativeManifest,
      testingValues: initialTestingValues,
    },
    resolvedManifest,
  } = useInitializedBuilderProject();
  const { getStoredMode } = useConnectorBuilderLocalStorage();
  const areDynamicStreamsEnabled = useExperiment("connectorBuilder.dynamicStreams");
  const isSchemaFormEnabled = useExperiment("connectorBuilder.schemaForm");

  const dynamicStreams = declarativeManifest?.manifest?.dynamic_streams;

  const hasDynamicStreams = Array.isArray(dynamicStreams) && dynamicStreams.length > 0;
  const initialTestStreamId =
    areDynamicStreamsEnabled && hasDynamicStreams
      ? { type: "dynamic_stream" as const, index: 0 }
      : { type: "stream" as const, index: 0 };

  const initialView = !isSchemaFormEnabled
    ? { type: "global" as const }
    : initialTestStreamId.type === "dynamic_stream"
    ? { type: "dynamic_stream" as const, index: 0 }
    : { type: "stream" as const, index: 0 };

  const values: BuilderState = {
    mode: failedInitialFormValueConversion
      ? isSchemaFormEnabled && resolvedManifest !== null
        ? getStoredMode(projectId)
        : "yaml"
      : getStoredMode(projectId),
    formValues: initialFormValues,
    yaml: initialYaml,
    customComponentsCode: componentsFileContent,
    name,
    view: initialView,
    streamTab: "requester" as const,
    testStreamId: initialTestStreamId,
    testingValues: initialTestingValues,
    manifest: resolvedManifest,
  };
  const initialValues = useRef(values);
  initialValues.current = values;

  return <BaseForm defaultValues={initialValues} />;
});
ConnectorBuilderEditPageInner.displayName = "ConnectorBuilderEditPageInner";

export const ConnectorBuilderEditPage: React.FC = () => (
  <ConnectorBuilderFormManagementStateProvider>
    <ConnectorBuilderLocalStorageProvider>
      <ConnectorBuilderEditPageInner />
    </ConnectorBuilderLocalStorageProvider>
  </ConnectorBuilderFormManagementStateProvider>
);

const BaseForm = React.memo(({ defaultValues }: { defaultValues: React.MutableRefObject<BuilderState> }) => {
  const { builderStateValidationSchema } = useBuilderValidationSchema();
  const isSchemaFormEnabled = useExperiment("connectorBuilder.schemaForm");

  // if this component re-renders, everything subscribed to rhf rerenders because the context object is a new one
  // To prevent this, the hook is placed in its own memoized component which only re-renders when necessary
  const methods = useForm({
    // @ts-expect-error TODO: connector builder team to fix this https://github.com/airbytehq/airbyte-internal-issues/issues/12252
    defaultValues: defaultValues.current,
    mode: "onChange",
    resolver: isSchemaFormEnabled ? undefined : yupResolver<AnyObjectSchema>(builderStateValidationSchema),
  });

  return (
    <FormProvider {...methods}>
      <ConnectorBuilderMainRHFContext.Provider value={methods}>
        <ConnectorBuilderFormStateProvider>
          <ConnectorBuilderTestReadProvider>
            <HeadTitle titles={[{ id: "connectorBuilder.title" }]} />
            <FlexContainer direction="column" gap="none" className={styles.container}>
              <MenuBar />
              <Panels />
            </FlexContainer>
          </ConnectorBuilderTestReadProvider>
        </ConnectorBuilderFormStateProvider>
      </ConnectorBuilderMainRHFContext.Provider>
    </FormProvider>
  );
});
BaseForm.displayName = "BaseForm";

const Panels = React.memo(() => {
  const mode = useBuilderWatch("mode");
  const { stateKey } = useConnectorBuilderFormManagementState();
  const isSchemaFormEnabled = useExperiment("connectorBuilder.schemaForm");

  return useMemo(
    () => (
      <ResizablePanels
        // key is used to force re-mount of the form when a different state version is loaded so the react-hook-form / YAML editor state is re-initialized with the new values
        key={stateKey}
        className={classnames(styles.panelsContainer, {
          [styles.gradientBg]: mode === "yaml",
          [styles.solidBg]: mode === "ui",
        })}
        panels={[
          {
            children:
              mode === "yaml" ? <YamlManifestEditor /> : isSchemaFormEnabled ? <SchemaFormBuilder /> : <Builder />,
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
    [isSchemaFormEnabled, mode, stateKey]
  );
});
Panels.displayName = "Panels";
