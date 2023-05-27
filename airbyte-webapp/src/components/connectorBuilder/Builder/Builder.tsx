import { useEffect, useMemo } from "react";
import React from "react";

import { BuilderView, useConnectorBuilderFormState } from "services/connectorBuilder/ConnectorBuilderStateService";

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

function getView(selectedView: BuilderView, hasMultipleStreams: boolean) {
  switch (selectedView) {
    case "global":
      return <GlobalConfigView />;
    case "inputs":
      return <InputsView />;
    default:
      // re-mount on changing stream
      return <StreamConfigView streamNum={selectedView} key={selectedView} hasMultipleStreams={hasMultipleStreams} />;
  }
}

export const Builder: React.FC<BuilderProps> = ({ hasMultipleStreams, toggleYamlEditor }) => {
  const { validateAndTouch } = useBuilderErrors();
  const { selectedView, blockedOnInvalidState } = useConnectorBuilderFormState();

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
