import cloneDeep from "lodash/cloneDeep";
import isEqual from "lodash/isEqual";
import { Range } from "monaco-editor";
import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useFormContext, get } from "react-hook-form";
import { useUpdateEffect } from "react-use";

import { CheckDynamicStreamType, CheckStreamType, ConnectorManifest } from "core/api/types/ConnectorManifest";
import { useConnectorBuilderSchema } from "core/services/connectorBuilder/ConnectorBuilderSchemaContext";
import { assertNever } from "core/utils/asserts";
import {
  useConnectorBuilderFormState,
  useConnectorBuilderFormManagementState,
} from "services/connectorBuilder/ConnectorBuilderStateService";

import styles from "./Builder.module.scss";
import { OAUTH_INPUT_SPEC_PATH } from "./BuilderDeclarativeOAuth";
import { BuilderSidebar } from "./BuilderSidebar";
import { ComponentsView } from "./ComponentsView";
import { DynamicStreamConfigView } from "./DynamicStreamConfigView";
import { GlobalConfigView } from "./GlobalConfigView";
import { InputModal, newInputInEditing } from "./InputModal";
import { InputsView } from "./InputsView";
import { StreamConfigView } from "./StreamConfigView";
import { BuilderFormInput, BuilderState } from "../types";
import { useAuthenticatorInputs, isHttpRequesterAuthenticator } from "../useAuthenticatorInputs";
import { useBuilderWatch } from "../useBuilderWatch";
import { useStreamNames } from "../useStreamNames";
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
        <UpdateCheckStreams />
        <div className={styles.builderView} ref={builderViewRef}>
          {getView(view, scrollToTop)}
        </div>
        {newUserInputContext && (
          <InputModal
            inputInEditing={newInputInEditing()}
            onClose={(newInput: BuilderFormInput | undefined) => {
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
  const { watch, trigger, getValues } = useFormContext();
  const setStreamToStale = useSetStreamToStale();
  const { updateUserInputsForAuth, updateUserInputsForDeclarativeOAuth, updateUserInputsForTokenUpdater } =
    useAuthenticatorInputs();

  const previousBuilderState = useRef(cloneDeep(getValues()));

  useEffect(() => {
    const subscription = watch((data, { name }) => {
      if (!name) {
        return;
      }

      trigger(name);
      if (data?.manifest) {
        registerChange(data.manifest as ConnectorManifest);
      }
      if (name.startsWith("manifest.streams.")) {
        const oldValue = get(previousBuilderState.current, name);
        const newValue = get(data, name);
        if (!isEqual(oldValue, newValue)) {
          const streamIndexString = name.match(/manifest\.streams\.(\d+)\..*/)?.[1];
          try {
            const streamIndex = streamIndexString ? parseInt(streamIndexString, 10) : undefined;
            if (streamIndex !== undefined) {
              setStreamToStale({ type: "stream", index: streamIndex });
            }
          } catch (error) {}
        }
      }
      if (name.endsWith("authenticator")) {
        const oldValue = get(previousBuilderState.current, name);
        const oldAuth = isHttpRequesterAuthenticator(oldValue) ? oldValue : undefined;
        const newValue = get(data, name);
        const newAuth = isHttpRequesterAuthenticator(newValue) ? newValue : undefined;
        if (oldAuth || newAuth) {
          updateUserInputsForAuth(oldAuth, newAuth);
        }
      }
      if (name === OAUTH_INPUT_SPEC_PATH) {
        const value = get(data, name);
        updateUserInputsForDeclarativeOAuth(value);
      }
      if (name.endsWith("authenticator.refresh_token_updater")) {
        const value = get(data, name);
        updateUserInputsForTokenUpdater(name, value);
      }

      previousBuilderState.current = cloneDeep(data);
    });
    return () => subscription.unsubscribe();
  }, [
    watch,
    setStreamToStale,
    registerChange,
    trigger,
    updateUserInputsForAuth,
    updateUserInputsForDeclarativeOAuth,
    updateUserInputsForTokenUpdater,
  ]);

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

/**
 * Updates the `check` field in the manifest to keep it in sync with the
 * current stream names in the manifest.
 */
const UpdateCheckStreams = () => {
  const { streamNames, dynamicStreamNames } = useStreamNames();
  const checkStreams = useBuilderWatch("manifest.check");
  const { setValue } = useFormContext();
  useUpdateEffect(() => {
    if (checkStreams.type === CheckDynamicStreamType.CheckDynamicStream) {
      if (dynamicStreamNames.length > 0) {
        return;
      }
      if (streamNames.length === 0) {
        return;
      }
      setValue("manifest.check", {
        type: CheckStreamType.CheckStream,
        stream_names: [streamNames[0]],
      });
      return;
    }

    const filteredCheckStaticStreamNames =
      checkStreams.stream_names?.filter((streamName) => streamNames.includes(streamName)) ?? [];

    if (
      filteredCheckStaticStreamNames.length > 0 &&
      filteredCheckStaticStreamNames.length !== checkStreams.stream_names?.length
    ) {
      setValue("manifest.check.stream_names", filteredCheckStaticStreamNames);
    } else if (filteredCheckStaticStreamNames.length === 0) {
      const nonEmptyStreamNames = streamNames.filter((streamName) => streamName.trim() !== "");
      if (nonEmptyStreamNames.length > 0) {
        setValue("manifest.check.stream_names", [nonEmptyStreamNames[0]]);
      } else {
        setValue("manifest.check.stream_names", []);
      }
    }
  }, [streamNames, dynamicStreamNames]);

  return null;
};
