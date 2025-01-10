import cloneDeep from "lodash/cloneDeep";
import debounce from "lodash/debounce";
import { Range } from "monaco-editor";
import React, { useEffect, useMemo } from "react";
import { AnyObjectSchema } from "yup";

import { removeEmptyProperties } from "core/utils/form";
import {
  useConnectorBuilderFormState,
  useConnectorBuilderFormManagementState,
} from "services/connectorBuilder/ConnectorBuilderStateService";

import styles from "./Builder.module.scss";
import { BuilderSidebar } from "./BuilderSidebar";
import { GlobalConfigView } from "./GlobalConfigView";
import { InputForm, newInputInEditing } from "./InputsForm";
import { InputsView } from "./InputsView";
import { StreamConfigView } from "./StreamConfigView";
import { BuilderFormValues, convertToManifest, useBuilderWatch } from "../types";
import { useBuilderErrors } from "../useBuilderErrors";
import { useBuilderValidationSchema } from "../useBuilderValidationSchema";

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

function cleanFormValues(values: unknown, builderFormValidationSchema: AnyObjectSchema) {
  return builderFormValidationSchema.cast(removeEmptyProperties(cloneDeep(values))) as unknown as BuilderFormValues;
}

export const Builder: React.FC<BuilderProps> = ({ hasMultipleStreams }) => {
  const { validateAndTouch } = useBuilderErrors();
  const {
    blockedOnInvalidState,
    updateJsonManifest,
    setFormValuesValid,
    setFormValuesDirty,
    undoRedo: { registerChange },
  } = useConnectorBuilderFormState();
  const { newUserInputContext, setNewUserInputContext } = useConnectorBuilderFormManagementState();
  const formValues = useBuilderWatch("formValues");
  const view = useBuilderWatch("view");

  const streams = useBuilderWatch("formValues.streams");
  const { builderFormValidationSchema } = useBuilderValidationSchema();

  const debouncedUpdateJsonManifest = useMemo(
    () =>
      debounce((values: BuilderFormValues) => {
        registerChange(cloneDeep(values));
        setFormValuesValid(builderFormValidationSchema.isValidSync(values));
        updateJsonManifest(convertToManifest(cleanFormValues(values, builderFormValidationSchema)));
        setFormValuesDirty(false);
      }, 500),
    [builderFormValidationSchema, registerChange, setFormValuesDirty, setFormValuesValid, updateJsonManifest]
  );

  useEffect(() => {
    setFormValuesDirty(true);
    debouncedUpdateJsonManifest(formValues);
  }, [debouncedUpdateJsonManifest, formValues, setFormValuesDirty]);

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
        <BuilderSidebar />
        <div className={styles.builderView}>{getView(selectedView, hasMultipleStreams)}</div>
        {newUserInputContext && (
          <InputForm
            inputInEditing={newInputInEditing()}
            onClose={(newInput) => {
              const { model, position } = newUserInputContext;
              setNewUserInputContext(undefined);
              if (!newInput) {
                // put cursor back to the original position by applying an empty edit
                model.applyEdits([
                  {
                    range: new Range(position.lineNumber, position.column, position.lineNumber, position.column),
                    text: "",
                    forceMoveMarkers: true,
                  },
                ]);
                return;
              }
              model.applyEdits([
                {
                  range: new Range(position.lineNumber, position.column, position.lineNumber, position.column),
                  text: `config['${newInput.key}']`,
                  forceMoveMarkers: true,
                },
              ]);
            }}
          />
        )}
      </div>
    ),
    [selectedView, hasMultipleStreams, newUserInputContext, setNewUserInputContext]
  );
};
