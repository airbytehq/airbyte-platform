import debounce from "lodash/debounce";
import { useEffect, useMemo, useRef, useState } from "react";
import { useFormContext, FieldPath } from "react-hook-form";
import { useIntl } from "react-intl";

import { Text } from "components/ui/Text";

import { useFormatError } from "core/errors";
import { useConnectorBuilderFormManagementState } from "services/connectorBuilder/ConnectorBuilderStateService";

import styles from "./BuilderYamlField.module.scss";
import { BuilderState, useBuilderWatch } from "../types";
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
  const formValue = useBuilderWatch(path);
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

  return (
    <>
      <div
        className={styles.yamlEditor}
        ref={(ref) => {
          elementRef.current = ref;
          // Call handler in here to make sure it handles new refs
          handleScrollToField(elementRef, path);
        }}
      >
        <YamlEditor
          value={localYamlValue}
          onChange={(val: string | undefined) => {
            setLocalYamlValue(val ?? "");
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
