import { useCallback, useMemo, useState } from "react";
import { useFormContext, useWatch } from "react-hook-form";
import { FormattedMessage, useIntl } from "react-intl";

import { FormLabel } from "components/forms/FormControl";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Input } from "components/ui/Input";
import { RemoveButton } from "components/ui/RemoveButton/RemoveButton";
import { Text } from "components/ui/Text";

import styles from "./AdditionalPropertiesControl.module.scss";
import { ControlGroup } from "./ControlGroup";
import { SchemaFormControl } from "./SchemaFormControl";
import { BaseControlComponentProps } from "./types";
import { useToggleConfig } from "./useToggleConfig";
import { useSchemaForm } from "../SchemaForm";
import { useErrorAtPath } from "../useErrorAtPath";

// Pattern to detect internal keys
const INTERNAL_KEY_PATTERN = /^_key\d+$/;

// Component to handle key input with internal state management
interface KeyInputProps {
  formKey: string;
  allFormKeys: string[];
  onKeyChange: (originalKey: string, newKey: string) => void;
}

const KeyInput: React.FC<KeyInputProps> = ({ formKey, allFormKeys, onKeyChange }) => {
  const { formatMessage } = useIntl();

  const [inputValue, setInputValue] = useState(INTERNAL_KEY_PATTERN.test(formKey) ? "" : formKey);
  const [isTouched, setIsTouched] = useState(false);

  const errorMessage = useMemo(() => {
    if (!isTouched) {
      return undefined;
    }

    if (inputValue.trim() === "") {
      return formatMessage({ id: "form.empty.error" });
    }

    const isDuplicate = allFormKeys.some(
      (otherFormKey: string) => otherFormKey !== formKey && otherFormKey === inputValue
    );
    if (isDuplicate) {
      return formatMessage({ id: "form.additionalProperties.duplicateKey" });
    }

    return undefined;
  }, [allFormKeys, formKey, formatMessage, inputValue, isTouched]);

  const handleChange = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => {
      setInputValue(e.target.value);
      setIsTouched(true);
    },
    [setInputValue, setIsTouched]
  );

  const handleBlur = useCallback(() => {
    setIsTouched(true);

    // Don't commit invalid or empty values, but keep the input state as-is for continued editing
    if (errorMessage || inputValue.trim() === "") {
      return;
    }

    // If value changed and is valid, commit it to the form
    if (inputValue !== formKey) {
      onKeyChange(formKey, inputValue);
    }
  }, [errorMessage, formKey, inputValue, onKeyChange]);

  return (
    <FlexContainer direction="column" gap="none" className={styles.keyInputContainer}>
      <FormLabel label={formatMessage({ id: "form.additionalProperties.key" })} htmlFor={formKey} />
      <Input id={formKey} value={inputValue} onChange={handleChange} onBlur={handleBlur} error={!!errorMessage} />
      {errorMessage && (
        <Text color="red" size="xs" className={styles.keyErrorMessage}>
          {errorMessage}
        </Text>
      )}
    </FlexContainer>
  );
};

export const AdditionalPropertiesControl = ({
  fieldSchema: additionalPropertiesSchema,
  baseProps,
  overrideByPath = {},
  skipRenderedPathRegistration = false,
  hideBorder = false,
}: BaseControlComponentProps) => {
  const { formatMessage } = useIntl();
  const { extractDefaultValuesFromSchema } = useSchemaForm();
  const { setValue } = useFormContext();
  const toggleConfig = useToggleConfig(baseProps.name, additionalPropertiesSchema);
  const error = useErrorAtPath(baseProps.name);

  // Get all current key-value pairs
  const rawFormValue = useWatch({ name: baseProps.name });
  const formValue = useMemo(() => rawFormValue ?? {}, [rawFormValue]);

  // Get all keys from the form value
  const formKeys = useMemo(() => Object.keys(formValue), [formValue]);

  // Add a new key-value pair
  const addPair = useCallback(() => {
    const internalKey = `_key${formKeys.length}`;
    const defaultValue = extractDefaultValuesFromSchema(additionalPropertiesSchema);

    // Add the pair to the form value with the internal key
    const updatedValue = { ...formValue, [internalKey]: defaultValue };
    setValue(baseProps.name, updatedValue);
  }, [
    formKeys.length,
    extractDefaultValuesFromSchema,
    additionalPropertiesSchema,
    formValue,
    setValue,
    baseProps.name,
  ]);

  // Remove a key-value pair
  const removePair = useCallback(
    (key: string) => {
      const updatedValue = { ...formValue };
      delete updatedValue[key];
      setValue(baseProps.name, updatedValue);
    },
    [baseProps.name, formValue, setValue]
  );

  // Handle a key change from KeyInput component
  const handleKeyChange = useCallback(
    (originalKey: string, newKey: string) => {
      if (originalKey === newKey) {
        return;
      }

      // Create a new object with the keys in the same order, replacing the key that changed
      const updatedValue: Record<string, unknown> = {};
      for (const currentKey of Object.keys(formValue)) {
        if (currentKey === originalKey) {
          updatedValue[newKey] = formValue[currentKey];
        } else {
          updatedValue[currentKey] = formValue[currentKey];
        }
      }

      // Update form value
      setValue(baseProps.name, updatedValue);
    },
    [baseProps.name, formValue, setValue]
  );

  const addButton = (
    <Button variant="secondary" onClick={addPair} type="button" icon="plus">
      <FormattedMessage id="form.additionalProperties.addKeyValuePair" />
    </Button>
  );

  const contents = (
    <>
      {Object.keys(formValue).map((key) => {
        return (
          <FlexContainer key={key} alignItems="flex-start" gap="md">
            <KeyInput formKey={key} allFormKeys={formKeys} onKeyChange={handleKeyChange} />
            <div className={styles.valueControlContainer}>
              <SchemaFormControl
                path={`${baseProps.name}.${key}`}
                overrideByPath={overrideByPath}
                skipRenderedPathRegistration={skipRenderedPathRegistration}
                fieldSchema={{
                  ...additionalPropertiesSchema,
                  // If the additionalPropertiesSchema has no title, show the standard Value title
                  title: additionalPropertiesSchema.title ?? formatMessage({ id: "form.additionalProperties.value" }),
                }}
                isRequired
              />
            </div>
            <RemoveButton className={styles.removeButton} onClick={() => removePair(key)} />
          </FlexContainer>
        );
      })}
      <div className={styles.addButtonContainer}>{addButton}</div>
    </>
  );

  return hideBorder ? (
    contents
  ) : (
    <ControlGroup
      title={baseProps.label}
      tooltip={baseProps.labelTooltip}
      path={baseProps.name}
      error={error}
      toggleConfig={baseProps.optional ? toggleConfig : undefined}
      header={baseProps.header}
      data-field-path={baseProps["data-field-path"]}
      disabled={baseProps.disabled}
    >
      {contents}
    </ControlGroup>
  );
};
