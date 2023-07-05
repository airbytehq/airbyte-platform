import { useEffect, useMemo } from "react";
import React from "react";

import { useConnectorBuilderFormState } from "services/connectorBuilder/ConnectorBuilderStateService";

import styles from "./Builder.module.scss";
import { BuilderSidebar } from "./BuilderSidebar";
import { GlobalConfigView } from "./GlobalConfigView";
import { InputsView } from "./InputsView";
import { StreamConfigView } from "./StreamConfigView";
import { useBuilderErrors } from "../useBuilderErrors";

interface BuilderProps {
  hasMultipleStreams: boolean;
  toggleYamlEditor: () => void;
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

export const Builder: React.FC<BuilderProps> = ({ hasMultipleStreams, toggleYamlEditor }) => {
  const { validateAndTouch } = useBuilderErrors();
  const {
    selectedView: selectedBuilderView,
    blockedOnInvalidState,
    builderFormValues,
  } = useConnectorBuilderFormState();
  const selectedView = useMemo(
    () =>
      selectedBuilderView !== "global" && selectedBuilderView !== "inputs"
        ? {
            streamNum: selectedBuilderView,
            streamId: builderFormValues.streams[selectedBuilderView]?.id ?? selectedBuilderView,
          }
        : selectedBuilderView,
    [builderFormValues.streams, selectedBuilderView]
  );

  useEffect(() => {
    if (blockedOnInvalidState) {
      validateAndTouch();
    }
  }, [blockedOnInvalidState, validateAndTouch]);

  return useMemo(
    () => (
      <div className={styles.container}>
        <BuilderSidebar className={styles.sidebar} toggleYamlEditor={toggleYamlEditor} />
        <form className={styles.form}>{getView(selectedView, hasMultipleStreams)}</form>
      </div>
    ),
    [hasMultipleStreams, selectedView, toggleYamlEditor]
  );
};
