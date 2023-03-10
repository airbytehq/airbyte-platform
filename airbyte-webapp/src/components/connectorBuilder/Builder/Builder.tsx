import { Form } from "formik";
import { useEffect } from "react";

import { BuilderView, useConnectorBuilderFormState } from "services/connectorBuilder/ConnectorBuilderStateService";

import styles from "./Builder.module.scss";
import { BuilderSidebar } from "./BuilderSidebar";
import { GlobalConfigView } from "./GlobalConfigView";
import { InputsView } from "./InputsView";
import { StreamConfigView } from "./StreamConfigView";
import { BuilderFormValues } from "../types";
import { useBuilderErrors } from "../useBuilderErrors";

interface BuilderProps {
  values: BuilderFormValues;
  validateForm: () => void;
  toggleYamlEditor: () => void;
}

function getView(selectedView: BuilderView, hasMultipleStreams: boolean) {
  switch (selectedView) {
    case "global":
      return <GlobalConfigView />;
    case "inputs":
      return <InputsView />;
    default:
      return <StreamConfigView streamNum={selectedView} hasMultipleStreams={hasMultipleStreams} />;
  }
}

export const Builder: React.FC<BuilderProps> = ({ values, toggleYamlEditor }) => {
  const { validateAndTouch } = useBuilderErrors();
  const { selectedView, blockedOnInvalidState } = useConnectorBuilderFormState();

  useEffect(() => {
    if (blockedOnInvalidState) {
      validateAndTouch();
    }
  }, [blockedOnInvalidState, validateAndTouch]);

  return (
    <div className={styles.container}>
      <BuilderSidebar className={styles.sidebar} toggleYamlEditor={toggleYamlEditor} />
      <Form className={styles.form}>{getView(selectedView, values.streams.length > 1)}</Form>
    </div>
  );
};
