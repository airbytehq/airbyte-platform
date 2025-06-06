import cloneDeep from "lodash/cloneDeep";
import debounce from "lodash/debounce";
import { Range } from "monaco-editor";
import React, { useCallback, useEffect, useMemo } from "react";
import { AnyObjectSchema } from "yup";

import { assertNever } from "core/utils/asserts";
import { removeEmptyProperties } from "core/utils/form";
import {
  useConnectorBuilderFormState,
  useConnectorBuilderFormManagementState,
} from "services/connectorBuilder__deprecated/ConnectorBuilderStateService";

import styles from "./Builder.module.scss";
import { BuilderSidebar } from "./BuilderSidebar";
import { ComponentsView } from "./ComponentsView";
import { DynamicStreamConfigView } from "./DynamicStreamConfigView";
import { GlobalConfigView } from "./GlobalConfigView";
import { InputForm, newInputInEditing } from "./InputsForm";
import { InputsView } from "./InputsView";
import { StreamConfigView } from "./StreamConfigView";
import { BuilderFormValues, BuilderState, convertToManifest } from "../types";
import { useBuilderErrors } from "../useBuilderErrors";
import { useBuilderValidationSchema } from "../useBuilderValidationSchema";
import { useBuilderWatch } from "../useBuilderWatch";

function getView(selectedView: BuilderState["view"], scrollToTop: () => void) {
  switch (selectedView.type) {
    case "global":
      return <GlobalConfigView />;
    case "inputs":
      return <InputsView />;
    case "components":
      return <ComponentsView />;
    case "dynamic_stream":
      return <DynamicStreamConfigView key={selectedView.index} streamId={selectedView} scrollToTop={scrollToTop} />;
    case "stream":
    case "generated_stream":
      return (
        <StreamConfigView
          streamId={selectedView}
          key={`${selectedView.type}-${selectedView.index}`}
          scrollToTop={scrollToTop}
        />
      );
    default:
      assertNever(selectedView);
  }
}

function cleanFormValues(values: unknown, builderFormValidationSchema: AnyObjectSchema) {
  return builderFormValidationSchema.cast(removeEmptyProperties(cloneDeep(values))) as unknown as BuilderFormValues;
}

export const Builder: React.FC = () => {
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

  const { builderFormValidationSchema } = useBuilderValidationSchema();

  // Create a reference to the builder view div for scrolling
  const builderViewRef = React.useRef<HTMLDivElement>(null);

  // Function to scroll the builder view to the top
  const scrollToTop = useCallback(() => {
    if (builderViewRef.current) {
      builderViewRef.current.scrollTo({ top: 0, behavior: "smooth" });
    }
  }, []);

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

  useEffect(() => {
    if (blockedOnInvalidState) {
      validateAndTouch();
    }
  }, [blockedOnInvalidState, validateAndTouch]);

  return useMemo(
    () => (
      <div className={styles.container}>
        <BuilderSidebar />
        <div className={styles.builderView} ref={builderViewRef}>
          {getView(view, scrollToTop)}
        </div>
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
    [view, newUserInputContext, setNewUserInputContext, scrollToTop]
  );
};
