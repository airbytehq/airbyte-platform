import isEqual from "lodash/isEqual";
import { Range } from "monaco-editor";
import React, { useCallback, useEffect, useMemo, useState } from "react";
import { useWatch, useFormContext, get } from "react-hook-form";
import { useIntl } from "react-intl";
import { useUpdateEffect } from "react-use";

import { SchemaForm } from "components/forms/SchemaForm/SchemaForm";
import { AirbyteJsonSchema } from "components/forms/SchemaForm/utils";

import { ConnectorManifest, DeclarativeComponentSchema } from "core/api/types/ConnectorManifest";
import { assertNever } from "core/utils/asserts";
import {
  useConnectorBuilderFormState,
  useConnectorBuilderFormManagementState,
} from "services/connectorBuilder/ConnectorBuilderStateService";
import { getPatternDescriptor } from "views/Connector/ConnectorForm/utils";

import styles from "./Builder.module.scss";
import { BuilderSidebar } from "./BuilderSidebar";
import { ComponentsView } from "./ComponentsView";
import { DynamicStreamConfigView } from "./DynamicStreamConfigView";
import { GeneratedStreamView } from "./GeneratedStreamView";
import { GlobalConfigView } from "./GlobalConfigView";
import { InputForm, newInputInEditing } from "./InputsForm";
import { InputsView } from "./InputsView";
import { StreamConfigView } from "./StreamConfigView";
import declarativeComponentSchema from "../../../../build/declarative_component_schema.yaml";
import { BuilderState, DEFAULT_JSON_MANIFEST_VALUES } from "../types";
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
      return <GeneratedStreamView streamId={selectedView} scrollToTop={scrollToTop} />;
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
          <ValidateTestingValues />
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
  const {
    updateJsonManifest,
    setFormValuesValid,
    undoRedo: { registerChange },
  } = useConnectorBuilderFormState();
  const { trigger, watch } = useFormContext();
  const setStreamToStale = useSetStreamToStale();
  const builderState = useWatch();
  // set stream to stale when it changes
  useEffect(() => {
    const subscription = watch((data, { name }) => {
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
  }, [watch, setStreamToStale, builderState, registerChange]);

  useEffect(() => {
    // The validation logic isn't updated until the next render cycle, so wait for that
    // before triggering validation and updating the builder state
    setTimeout(() => {
      trigger().then((isValid) => {
        setFormValuesValid(isValid);
        updateJsonManifest(builderState.manifest ?? DEFAULT_JSON_MANIFEST_VALUES);
      });
    }, 0);
  }, [builderState, setFormValuesValid, trigger, updateJsonManifest]);

  return null;
};

const ValidateTestingValues = () => {
  const { formatMessage } = useIntl();
  const { register, unregister, clearErrors } = useFormContext();
  const spec = useBuilderWatch("manifest.spec");
  const [registeredFields, setRegisteredFields] = useState<Set<string>>(new Set());
  const specSchema = spec?.connection_specification as AirbyteJsonSchema | undefined;

  useEffect(() => {
    const properties = specSchema?.properties as Record<string, AirbyteJsonSchema> | undefined;
    if (specSchema && properties) {
      const specFields = Object.keys(properties);
      const required: string[] = isArrayOfStrings(specSchema.required) ? specSchema.required : [];

      const allFields = new Set([...specFields, ...registeredFields]);
      allFields.forEach((field) => {
        const path = `testingValues.${field}`;
        if (!specFields.includes(field)) {
          unregister(path);
          // Must wait until next render cycle when field is unregistered to clear errors,
          // otherwise the errors will return
          setTimeout(() => {
            clearErrors(path);
          }, 0);
          setRegisteredFields((current) => {
            current.delete(field);
            return current;
          });
          return;
        }

        const validatePattern = (value: unknown) => {
          const pattern = properties[field].pattern;
          if (pattern && typeof value === "string") {
            const regex = new RegExp(pattern);
            if (!regex.test(value)) {
              return formatMessage({ id: "form.pattern.error" }, { pattern: getPatternDescriptor(properties[field]) });
            }
          }
          return true;
        };

        if (required.includes(field)) {
          register(path, {
            validate: (value) => {
              if (value === undefined || value === null || value === "") {
                return formatMessage({ id: "form.empty.error" });
              }
              return validatePattern(value);
            },
          });
        } else {
          register(path, {
            validate: (value) => validatePattern(value),
          });
        }
        setRegisteredFields((current) => {
          current.add(field);
          return current;
        });
      });
    }
  }, [formatMessage, register, registeredFields, unregister, clearErrors, specSchema]);

  return null;
};

function isArrayOfStrings(value: unknown): value is string[] {
  return Array.isArray(value) && value.every((item) => typeof item === "string");
}
