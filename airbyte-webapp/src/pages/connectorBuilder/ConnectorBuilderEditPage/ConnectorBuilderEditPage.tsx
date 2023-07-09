import { yupResolver } from "@hookform/resolvers/yup";
import classnames from "classnames";
import debounce from "lodash/debounce";
import isEqual from "lodash/isEqual";
import React, { useCallback, useEffect, useMemo, useRef } from "react";
import { FormProvider, useForm, useWatch } from "react-hook-form";
import { useIntl } from "react-intl";
import { AnyObjectSchema } from "yup";

import { HeadTitle } from "components/common/HeadTitle";
import { Builder } from "components/connectorBuilder/Builder/Builder";
import { StreamTestingPanel } from "components/connectorBuilder/StreamTestingPanel";
import { builderFormValidationSchema, BuilderFormValues } from "components/connectorBuilder/types";
import { YamlEditor } from "components/connectorBuilder/YamlEditor";
import { ResizablePanels } from "components/ui/ResizablePanels";

import { Action, Namespace } from "core/services/analytics";
import { useAnalyticsService } from "core/services/analytics";
import { ConnectorBuilderLocalStorageProvider } from "services/connectorBuilder/ConnectorBuilderLocalStorageService";
import {
  ConnectorBuilderTestReadProvider,
  ConnectorBuilderFormStateProvider,
  useConnectorBuilderFormState,
  ConnectorBuilderFormManagementStateProvider,
  ConnectorBuilderMainRHFContext,
} from "services/connectorBuilder/ConnectorBuilderStateService";
import { removeEmptyProperties } from "utils/form";

import styles from "./ConnectorBuilderEditPage.module.scss";

const ConnectorBuilderEditPageInner: React.FC = React.memo(() => {
  const { builderFormValues, editorView, setEditorView, stateKey } = useConnectorBuilderFormState();
  const analyticsService = useAnalyticsService();

  useEffect(() => {
    analyticsService.track(Namespace.CONNECTOR_BUILDER, Action.CONNECTOR_BUILDER_EDIT, {
      actionDescription: "Connector Builder UI /edit page opened",
    });
  }, [analyticsService]);

  const switchToUI = useCallback(() => setEditorView("ui"), [setEditorView]);
  const switchToYaml = useCallback(() => setEditorView("yaml"), [setEditorView]);

  const initialFormValues = useRef(builderFormValues);
  initialFormValues.current = builderFormValues;

  return (
    <BaseForm
      // key is used to force re-mount of the form when a different state version is loaded so the react-hook-form / YAML editor state is re-initialized with the new values
      key={stateKey}
      editorView={editorView}
      switchToUI={switchToUI}
      switchToYaml={switchToYaml}
      defaultValues={initialFormValues}
    />
  );
});

export const ConnectorBuilderEditPage: React.FC = () => (
  <ConnectorBuilderFormManagementStateProvider>
    <ConnectorBuilderLocalStorageProvider>
      <ConnectorBuilderFormStateProvider>
        <ConnectorBuilderTestReadProvider>
          <HeadTitle titles={[{ id: "connectorBuilder.title" }]} />
          <ConnectorBuilderEditPageInner />
        </ConnectorBuilderTestReadProvider>
      </ConnectorBuilderFormStateProvider>
    </ConnectorBuilderLocalStorageProvider>
  </ConnectorBuilderFormManagementStateProvider>
);
ConnectorBuilderEditPageInner.displayName = "ConnectorBuilderEditPageInner";

const BaseForm = React.memo(
  ({
    editorView,
    switchToUI,
    switchToYaml,
    defaultValues,
  }: {
    editorView: string;
    switchToUI: () => void;
    switchToYaml: () => void;
    defaultValues: React.MutableRefObject<BuilderFormValues>;
  }) => {
    // if this component re-renders, everything subscribed to rhf rerenders because the context object is a new one
    // Do prevent this, the hook is placed in its own memoized component which only re-renders when necessary
    const methods = useForm({
      defaultValues: defaultValues.current,
      mode: "onChange",
      resolver: yupResolver<AnyObjectSchema>(builderFormValidationSchema),
    });

    return (
      <FormProvider {...methods}>
        <ConnectorBuilderMainRHFContext.Provider value={methods}>
          <Panels editorView={editorView} switchToUI={switchToUI} switchToYaml={switchToYaml} />
        </ConnectorBuilderMainRHFContext.Provider>
      </FormProvider>
    );
  }
);

BaseForm.displayName = "BaseForm";

function cleanedFormValues(values: unknown) {
  return builderFormValidationSchema.cast(removeEmptyProperties(values)) as unknown as BuilderFormValues;
}

const Panels = React.memo(
  ({
    editorView,
    switchToUI,
    switchToYaml,
  }: {
    editorView: string;
    switchToUI: () => void;
    switchToYaml: () => void;
  }) => {
    const { formatMessage } = useIntl();
    const { setBuilderFormValues } = useConnectorBuilderFormState();

    const values = useWatch();

    const lastUpdatedValues = useRef<BuilderFormValues | null>(null);
    if (lastUpdatedValues.current === null) {
      lastUpdatedValues.current = cleanedFormValues(values);
    }

    const debouncedSetBuilderFormValues = useMemo(
      () =>
        debounce((values) => {
          const newFormValues = cleanedFormValues(values);
          if (isEqual(lastUpdatedValues.current, newFormValues)) {
            return;
          }

          lastUpdatedValues.current = newFormValues;
          // update upstream state
          setBuilderFormValues(newFormValues, builderFormValidationSchema.isValidSync(newFormValues));
        }, 200),
      [setBuilderFormValues]
    );

    useEffect(() => {
      debouncedSetBuilderFormValues(values);
    }, [values, debouncedSetBuilderFormValues]);

    return useMemo(
      () => (
        <ResizablePanels
          className={classnames({ [styles.gradientBg]: editorView === "yaml", [styles.solidBg]: editorView === "ui" })}
          firstPanel={{
            children: (
              <>
                {editorView === "yaml" ? (
                  <YamlEditor toggleYamlEditor={switchToUI} />
                ) : (
                  <Builder hasMultipleStreams={values.streams.length > 1} toggleYamlEditor={switchToYaml} />
                )}
              </>
            ),
            className: styles.leftPanel,
            minWidth: 550,
          }}
          secondPanel={{
            children: <StreamTestingPanel />,
            className: styles.rightPanel,
            flex: 0.33,
            minWidth: 60,
            overlay: {
              displayThreshold: 325,
              header: formatMessage({ id: "connectorBuilder.testConnector" }),
              rotation: "counter-clockwise",
            },
          }}
        />
      ),
      [editorView, formatMessage, switchToUI, switchToYaml, values.streams.length]
    );
  }
);
Panels.displayName = "Panels";
