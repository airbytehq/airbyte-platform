import { Range } from "monaco-editor";
import React, { useCallback, useEffect, useMemo } from "react";
import { useWatch, useFormContext } from "react-hook-form";

import { SchemaForm } from "components/forms/SchemaForm/SchemaForm";
import { AirbyteJsonSchema } from "components/forms/SchemaForm/utils";

import { ConnectorManifest, DeclarativeComponentSchema } from "core/api/types/ConnectorManifest";
import { assertNever } from "core/utils/asserts";
import {
  useConnectorBuilderFormState,
  useConnectorBuilderFormManagementState,
} from "services/connectorBuilder/ConnectorBuilderStateService";

import styles from "./Builder.module.scss";
import { BuilderSidebar } from "./BuilderSidebar";
import { ComponentsView } from "./ComponentsView";
import { DynamicStreamConfigView } from "./DynamicStreamConfigView";
import { GlobalConfigView } from "./GlobalConfigView";
import { InputForm, newInputInEditing } from "./InputsForm";
import { InputsView } from "./InputsView";
import { StreamConfigView } from "./StreamConfigView";
import declarativeComponentSchema from "../../../../build/declarative_component_schema.yaml";
import { BuilderState } from "../types";
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

export const Builder: React.FC = () => {
  const { newUserInputContext, setNewUserInputContext } = useConnectorBuilderFormManagementState();
  const view = useBuilderWatch("view");

  // Create a reference to the builder view div for scrolling
  const builderViewRef = React.useRef<HTMLDivElement>(null);

  // Function to scroll the builder view to the top
  const scrollToTop = useCallback(() => {
    if (builderViewRef.current) {
      builderViewRef.current.scrollTo({ top: 0, behavior: "smooth" });
    }
  }, []);

  return useMemo(
    () => (
      <div className={styles.container}>
        <BuilderSidebar />
        <SchemaForm<AirbyteJsonSchema, DeclarativeComponentSchema>
          schema={declarativeComponentSchema}
          nestedUnderPath="manifest"
          refTargetPath="manifest.definitions.linked"
          onlyShowErrorIfTouched
        >
          <SyncValuesToBuilderState />
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
        </SchemaForm>
      </div>
    ),
    [view, newUserInputContext, setNewUserInputContext, scrollToTop]
  );
};

const SyncValuesToBuilderState = () => {
  const { updateJsonManifest, setFormValuesValid } = useConnectorBuilderFormState();
  const { trigger } = useFormContext();
  const schemaFormValues = useWatch({ name: "manifest" }) as ConnectorManifest;

  useEffect(() => {
    // The validation logic isn't updated until the next render cycle, so wait for that
    // before triggering validation and updating the builder state
    setTimeout(() => {
      trigger().then((isValid) => {
        setFormValuesValid(isValid);
        updateJsonManifest(schemaFormValues);
      });
    }, 0);
  }, [schemaFormValues, setFormValuesValid, trigger, updateJsonManifest]);

  return null;
};
