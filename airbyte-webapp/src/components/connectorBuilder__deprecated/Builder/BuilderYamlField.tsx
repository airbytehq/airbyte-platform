import debounce from "lodash/debounce";
import isEqual from "lodash/isEqual";
import { useEffect, useMemo, useRef, useState } from "react";
import { useFormContext, FieldPath } from "react-hook-form";
import { useIntl } from "react-intl";

import { Text } from "components/ui/Text";

import { useFormatError } from "core/errors";
import { useConnectorBuilderFormManagementState } from "services/connectorBuilder__deprecated/ConnectorBuilderStateService";

import styles from "./BuilderYamlField.module.scss";
import { BuilderState } from "../types";
import { useBuilderWatchWithPreview } from "../useBuilderWatch";
import { YamlEditor } from "../YamlEditor";

interface BuilderYamlFieldProps {
  path: FieldPath<BuilderState>;
  setLocalYamlIsDirty?: (isDirty: boolean) => void;
}

export const BuilderYamlField: React.FC<BuilderYamlFieldProps> = ({ path, setLocalYamlIsDirty }) => {
  const { formatMessage } = useIntl();
  const formatError = useFormatError();
  const { setValue, register, getFieldState } = useFormContext();
  const { error } = getFieldState(path);
  const { fieldValue: formValue, isPreview } = useBuilderWatchWithPreview(path);
  const pathString = path as string;

  // Use a separate state for the YamlEditor value to avoid the debouncedSetValue
  // causing the YamlEditor be set to a previous value while still typing
  const [localYamlValue, setLocalYamlValue] = useState(formValue);
  const debouncedSetValue = useMemo(
    () =>
      debounce((...args: Parameters<typeof setValue>) => {
        setValue(...args);
        setLocalYamlIsDirty?.(false);
      }, 500),
    [setLocalYamlIsDirty, setValue]
  );

  const elementRef = useRef<HTMLDivElement | null>(null);
  const { handleScrollToField } = useConnectorBuilderFormManagementState();
  useEffect(() => {
    // Call handler in here to make sure it handles new scrollToField value from the context
    handleScrollToField(elementRef, pathString);
  }, [handleScrollToField, pathString]);

  // Update the local value when the form value changes, so that undo/redo has an effect here
  useEffect(() => {
    setLocalYamlValue(formValue);
  }, [formValue]);

  return (
    <>
      <div className={styles.yamlEditor} ref={elementRef}>
        <YamlEditor
          value={String(localYamlValue)}
          readOnly={isPreview}
          onChange={(val: string | undefined) => {
            setLocalYamlValue(val);
            // If both values are empty or equal, don't set the form value to avoid triggering an unwanted form value change.
            // This is needed because if the user undoes a change, causing the form value and this editor to be set to undefined/empty,
            // then calling setValue would cause the empty string to be set on this field instead, which would delete the redo history.
            if ((!val && !formValue) || isEqual(val, formValue)) {
              return;
            }
            setLocalYamlIsDirty?.(true);
            debouncedSetValue(path, val, {
              shouldValidate: true,
              shouldDirty: true,
              shouldTouch: true,
            });
          }}
          onMount={(_) => {
            // register path so that validation rules are applied
            register(path);
          }}
          bubbleUpUndoRedo
        />
      </div>
      {error && (
        <Text color="red" className={styles.yamlError}>
          {formatError({
            ...error,
            name: "YamlComponentError",
            message: error.message || formatMessage({ id: "connectorBuilder.defaultYamlError" }),
          })}
        </Text>
      )}
    </>
  );
};
