import { Range } from "monaco-editor";
import React, { useCallback, useEffect, useMemo, useState } from "react";
import { useWatch, useFormContext } from "react-hook-form";
import { useUpdateEffect } from "react-use";

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
  const [previousView, setPreviousView] = useState<BuilderState["view"]>(view);

  // Create a reference to the builder view div for scrolling
  const builderViewRef = React.useRef<HTMLDivElement>(null);

  // Function to scroll the builder view to the top
  const scrollToTop = useCallback(() => {
    if (builderViewRef.current) {
      builderViewRef.current.scrollTo({ top: 0, behavior: "auto" });
    }
  }, []);

  // Scroll to the top when the view changes
  useUpdateEffect(() => {
    if (view.type === "generated_stream") {
      if (
        previousView.type === "generated_stream" &&
        view.dynamicStreamName === previousView.dynamicStreamName &&
        view.index === previousView.index
      ) {
        return;
      }
    } else if (view.type === "stream") {
      if (previousView.type === "stream" && view.index === previousView.index) {
        return;
      }
    } else if (view.type === "dynamic_stream") {
      if (previousView.type === "dynamic_stream" && view.index === previousView.index) {
        return;
      }
    } else if (previousView.type === view.type) {
      return;
    }

    scrollToTop();
    setPreviousView(view);
  }, [view]);

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
    [newUserInputContext, view, scrollToTop, setNewUserInputContext]
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
