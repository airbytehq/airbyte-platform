import classnames from "classnames";
import React, { useMemo, useRef } from "react";
import { FormProvider, useForm } from "react-hook-form";

import { Builder } from "components/connectorBuilder/Builder/Builder";
import { MenuBar } from "components/connectorBuilder/MenuBar";
import { StreamTestingPanel } from "components/connectorBuilder/StreamTestingPanel";
import { BuilderState, DEFAULT_JSON_MANIFEST_VALUES_WITH_STREAM } from "components/connectorBuilder/types";
import { useBuilderWatch } from "components/connectorBuilder/useBuilderWatch";
import { YamlManifestEditor } from "components/connectorBuilder/YamlEditor";
import { HeadTitle } from "components/HeadTitle";
import { FlexContainer } from "components/ui/Flex";
import { ResizablePanels } from "components/ui/ResizablePanels";

import {
  ConnectorBuilderResolveProvider,
  useConnectorBuilderResolve,
} from "core/services/connectorBuilder/ConnectorBuilderResolveContext";
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
  const initialValues = useRef(values);
  initialValues.current = values;

  return <BaseForm defaultValues={initialValues} />;
});
ConnectorBuilderEditPageInner.displayName = "ConnectorBuilderEditPageInner";

export const ConnectorBuilderEditPage: React.FC = () => (
  <ConnectorBuilderFormManagementStateProvider>
    <ConnectorBuilderLocalStorageProvider>
      <ConnectorBuilderResolveProvider>
        <ConnectorBuilderEditPageInner />
      </ConnectorBuilderResolveProvider>
    </ConnectorBuilderLocalStorageProvider>
  </ConnectorBuilderFormManagementStateProvider>
);

const BaseForm = React.memo(({ defaultValues }: { defaultValues: React.MutableRefObject<BuilderState> }) => {
  // if this component re-renders, everything subscribed to rhf rerenders because the context object is a new one
  // To prevent this, the hook is placed in its own memoized component which only re-renders when necessary
  const methods = useForm({
    // @ts-expect-error TODO: connector builder team to fix this https://github.com/airbytehq/airbyte-internal-issues/issues/12252
    defaultValues: defaultValues.current,
    mode: "onChange",
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
