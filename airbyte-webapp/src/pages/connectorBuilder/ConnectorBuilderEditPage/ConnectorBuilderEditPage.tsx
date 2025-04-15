import { yupResolver } from "@hookform/resolvers/yup";
import classnames from "classnames";
import React, { useMemo, useRef } from "react";
import { FormProvider, useForm } from "react-hook-form";
import { AnyObjectSchema } from "yup";

import { Builder } from "components/connectorBuilder/Builder/Builder";
import { MenuBar } from "components/connectorBuilder/MenuBar";
import { StreamTestingPanel } from "components/connectorBuilder/StreamTestingPanel";
import { BuilderState } from "components/connectorBuilder/types";
import { useBuilderValidationSchema } from "components/connectorBuilder/useBuilderValidationSchema";
import { useBuilderWatch } from "components/connectorBuilder/useBuilderWatch";
import { YamlManifestEditor } from "components/connectorBuilder/YamlEditor";
import { HeadTitle } from "components/HeadTitle";
import { FlexContainer } from "components/ui/Flex";
import { ResizablePanels } from "components/ui/ResizablePanels";

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
      testingValues: initialTestingValues,
    },
  } = useInitializedBuilderProject();
  const { getStoredMode } = useConnectorBuilderLocalStorage();
  const values = {
    mode: failedInitialFormValueConversion ? "yaml" : getStoredMode(projectId),
    formValues: initialFormValues,
    yaml: initialYaml,
    customComponentsCode: componentsFileContent,
    name,
    view: "global" as const,
    streamTab: "requester" as const,
    testStreamIndex: 0,
    testingValues: initialTestingValues,
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

  // if this component re-renders, everything subscribed to rhf rerenders because the context object is a new one
  // Do prevent this, the hook is placed in its own memoized component which only re-renders when necessary
  const methods = useForm({
    // @ts-expect-error TODO: connector builder team to fix this https://github.com/airbytehq/airbyte-internal-issues/issues/12252
    defaultValues: defaultValues.current,
    mode: "onChange",
    resolver: yupResolver<AnyObjectSchema>(builderStateValidationSchema),
  });

  return (
    <FormProvider {...methods}>
      <ConnectorBuilderMainRHFContext.Provider value={methods}>
        <form
          className={styles.form}
          onSubmit={(e) => {
            // prevent form submission
            e.preventDefault();
          }}
        >
          <ConnectorBuilderFormStateProvider>
            <ConnectorBuilderTestReadProvider>
              <HeadTitle titles={[{ id: "connectorBuilder.title" }]} />
              <FlexContainer direction="column" gap="none" className={styles.container}>
                <MenuBar />
                <Panels />
              </FlexContainer>
            </ConnectorBuilderTestReadProvider>
          </ConnectorBuilderFormStateProvider>
        </form>
      </ConnectorBuilderMainRHFContext.Provider>
    </FormProvider>
  );
});
BaseForm.displayName = "BaseForm";

const Panels = React.memo(() => {
  const mode = useBuilderWatch("mode");
  const { stateKey } = useConnectorBuilderFormManagementState();

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
            children: <>{mode === "yaml" ? <YamlManifestEditor /> : <Builder />}</>,
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
    [mode, stateKey]
  );
});
Panels.displayName = "Panels";
