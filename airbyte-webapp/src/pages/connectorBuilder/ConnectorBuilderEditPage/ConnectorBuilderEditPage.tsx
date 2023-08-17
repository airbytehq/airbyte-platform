import { yupResolver } from "@hookform/resolvers/yup";
import classnames from "classnames";
import React, { useMemo, useRef } from "react";
import { FormProvider, useForm } from "react-hook-form";
import { useIntl } from "react-intl";
import { AnyObjectSchema } from "yup";

import { HeadTitle } from "components/common/HeadTitle";
import { Builder } from "components/connectorBuilder/Builder/Builder";
import { StreamTestingPanel } from "components/connectorBuilder/StreamTestingPanel";
import { BuilderState, builderStateValidationSchema, useBuilderWatch } from "components/connectorBuilder/types";
import { YamlEditor } from "components/connectorBuilder/YamlEditor";
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
    initialFormValues,
    failedInitialFormValueConversion,
    initialYaml,
    builderProject: {
      builderProject: { name },
    },
  } = useInitializedBuilderProject();
  const { storedMode } = useConnectorBuilderLocalStorage();
  const values = {
    mode: failedInitialFormValueConversion ? "yaml" : storedMode,
    formValues: initialFormValues,
    yaml: initialYaml,
    name,
    view: "global" as const,
    testStreamIndex: 0,
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
  // if this component re-renders, everything subscribed to rhf rerenders because the context object is a new one
  // Do prevent this, the hook is placed in its own memoized component which only re-renders when necessary
  const methods = useForm({
    defaultValues: defaultValues.current,
    mode: "onChange",
    resolver: yupResolver<AnyObjectSchema>(builderStateValidationSchema),
  });

  return (
    <FormProvider {...methods}>
      <ConnectorBuilderMainRHFContext.Provider value={methods}>
        <form className={styles.form}>
          <ConnectorBuilderFormStateProvider>
            <ConnectorBuilderTestReadProvider>
              <HeadTitle titles={[{ id: "connectorBuilder.title" }]} />
              <Panels />
            </ConnectorBuilderTestReadProvider>
          </ConnectorBuilderFormStateProvider>
        </form>
      </ConnectorBuilderMainRHFContext.Provider>
    </FormProvider>
  );
});
BaseForm.displayName = "BaseForm";

const Panels = React.memo(() => {
  const { formatMessage } = useIntl();
  const formValues = useBuilderWatch("formValues");
  const mode = useBuilderWatch("mode");
  const { stateKey } = useConnectorBuilderFormManagementState();

  return useMemo(
    () => (
      <ResizablePanels
        // key is used to force re-mount of the form when a different state version is loaded so the react-hook-form / YAML editor state is re-initialized with the new values
        key={stateKey}
        className={classnames({ [styles.gradientBg]: mode === "yaml", [styles.solidBg]: mode === "ui" })}
        panels={[
          {
            children: (
              <>{mode === "yaml" ? <YamlEditor /> : <Builder hasMultipleStreams={formValues.streams.length > 1} />}</>
            ),
            className: styles.leftPanel,
            minWidth: 550,
          },
          {
            children: <StreamTestingPanel />,
            className: styles.rightPanel,
            flex: 0.33,
            minWidth: 60,
            overlay: {
              displayThreshold: 325,
              header: formatMessage({ id: "connectorBuilder.testConnector" }),
              rotation: "counter-clockwise",
            },
          },
        ]}
      />
    ),
    [formValues.streams.length, formatMessage, mode, stateKey]
  );
});
Panels.displayName = "Panels";
