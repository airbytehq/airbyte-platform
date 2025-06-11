import isEqual from "lodash/isEqual";
import { Range } from "monaco-editor";
import React, { useCallback, useEffect, useMemo, useState } from "react";
import { useWatch, useFormContext, get } from "react-hook-form";
import { useUpdateEffect } from "react-use";

import { ConnectorManifest } from "core/api/types/ConnectorManifest";
import { useConnectorBuilderSchema } from "core/services/connectorBuilder/ConnectorBuilderSchemaContext";
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
import { BuilderState } from "../types";
import { useBuilderWatch } from "../useBuilderWatch";
import { useSetStreamToStale } from "../useStreamTestMetadata";

function getView(selectedView: BuilderState["view"], scrollToTop: () => void) {
  switch (selectedView.type) {
    case "global":
      return <GlobalConfigView />;
    case "inputs":
      return <InputsView />;
    case "components":
      return <ComponentsView />;
    case "dynamic_stream":
      return <DynamicStreamConfigView key={selectedView.index} streamId={selectedView} />;
    case "stream":
      return (
        <StreamConfigView
          streamId={selectedView}
          key={`${selectedView.type}-${selectedView.index}`}
          scrollToTop={scrollToTop}
        />
      );
    case "generated_stream":
      return (
        <StreamConfigView
          key={`${selectedView.type}-${selectedView.dynamicStreamName}-${selectedView.index}`}
          streamId={selectedView}
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
        <TriggerStateEffects />
        <UpdateSchemaForTestingValues />
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
    [newUserInputContext, view, scrollToTop, setNewUserInputContext]
  );
};

const TriggerStateEffects = () => {
  const {
    undoRedo: { registerChange },
  } = useConnectorBuilderFormState();
  const { watch, trigger } = useFormContext();
  const setStreamToStale = useSetStreamToStale();
  const builderState = useWatch();
  useEffect(() => {
    const subscription = watch((data, { name }) => {
      if (name) {
        trigger(name);
      }
      if (data?.manifest) {
        registerChange(data.manifest as ConnectorManifest);
      }
      if (name?.startsWith("manifest.streams.")) {
        const oldValue = get(builderState, name);
        const newValue = get(data, name);
        if (!isEqual(oldValue, newValue)) {
          const streamIndexString = name.match(/manifest\.streams\.(\d+)\..*/)?.[1];
          try {
            const streamIndex = streamIndexString ? parseInt(streamIndexString, 10) : undefined;
            if (streamIndex) {
              setStreamToStale({ type: "stream", index: streamIndex });
            }
          } catch (error) {}
        }
      }
    });
    return () => subscription.unsubscribe();
  }, [watch, setStreamToStale, builderState, registerChange, trigger]);

  return null;
};

/**
 * When the spec schema changes, we must update the schema for testing values
 * to match it so that the testing values inputs are properly validated.
 *
 * We also need to trigger a re-validation of the testing values after the state
 * schema gets updated to ensure the errors are up to date.
 */
const UpdateSchemaForTestingValues = () => {
  const { builderStateSchema, setBuilderStateSchema } = useConnectorBuilderSchema();
  const { trigger } = useFormContext();
  const specSchema = useBuilderWatch("manifest.spec.connection_specification");

  useEffect(() => {
    if (specSchema) {
      setBuilderStateSchema((prevSchema) => ({
        ...prevSchema,
        properties: {
          ...prevSchema.properties,
          testingValues: specSchema,
        },
      }));
    }
  }, [specSchema, setBuilderStateSchema, trigger]);

  useUpdateEffect(() => {
    trigger("testingValues");
  }, [builderStateSchema]);

  return null;
};
