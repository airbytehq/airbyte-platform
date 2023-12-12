import debounce from "lodash/debounce";
import React, { useEffect, useMemo } from "react";

import { removeEmptyProperties } from "core/utils/form";
import { useConnectorBuilderFormState } from "services/connectorBuilder/ConnectorBuilderStateService";

import styles from "./Builder.module.scss";
import { BuilderSidebar } from "./BuilderSidebar";
import { GlobalConfigView } from "./GlobalConfigView";
import { InputsView } from "./InputsView";
import { StreamConfigView } from "./StreamConfigView";
import { BuilderFormValues, builderFormValidationSchema, convertToManifest, useBuilderWatch } from "../types";
import { useBuilderErrors } from "../useBuilderErrors";

interface BuilderProps {
  hasMultipleStreams: boolean;
}

function getView(
  selectedView: "global" | "inputs" | { streamNum: number; streamId: string },
  hasMultipleStreams: boolean
) {
  switch (selectedView) {
    case "global":
      return <GlobalConfigView />;
    case "inputs":
      return <InputsView />;
    default:
      // re-mount on changing stream
      return (
        <StreamConfigView
          streamNum={selectedView.streamNum}
          key={selectedView.streamId}
          hasMultipleStreams={hasMultipleStreams}
        />
      );
  }
}

function cleanedFormValues(values: unknown) {
  return builderFormValidationSchema.cast(removeEmptyProperties(values)) as unknown as BuilderFormValues;
}

export const Builder: React.FC<BuilderProps> = ({ hasMultipleStreams }) => {
  const { validateAndTouch } = useBuilderErrors();
  const { blockedOnInvalidState, updateJsonManifest, setFormValuesValid } = useConnectorBuilderFormState();
  const formValues = useBuilderWatch("formValues");
  const view = useBuilderWatch("view");
  const streams = useBuilderWatch("formValues.streams");

  const debouncedUpdateJsonManifest = useMemo(
    () =>
      debounce((values) => {
        setFormValuesValid(builderFormValidationSchema.isValidSync(values));
        updateJsonManifest(convertToManifest(cleanedFormValues(values)));
      }, 200),
    [setFormValuesValid, updateJsonManifest]
  );

  useEffect(() => {
    debouncedUpdateJsonManifest(formValues);
  }, [debouncedUpdateJsonManifest, formValues]);

  const selectedView = useMemo(
    () =>
      view !== "global" && view !== "inputs"
        ? {
            streamNum: view,
            streamId: streams[view]?.id ?? view,
          }
        : view,
    [streams, view]
  );

  useEffect(() => {
    if (blockedOnInvalidState) {
      validateAndTouch();
    }
  }, [blockedOnInvalidState, validateAndTouch]);

  return useMemo(
    () => (
      <div className={styles.container}>
        <BuilderSidebar className={styles.sidebar} />
        <div className={styles.builderView}>{getView(selectedView, hasMultipleStreams)}</div>
      </div>
    ),
    [hasMultipleStreams, selectedView]
  );
};
