import { useCallback, useRef, useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { FormLabel } from "components/ui/forms/FormControl";
import { Input } from "components/ui/Input";
import { SecretTextArea } from "components/ui/SecretTextArea";
import { TextArea } from "components/ui/TextArea";

import styles from "./SecretField.module.scss";

interface SecretFieldProps {
  name: string;
  id: string;
  value: string;
  onUpdate: (value: string) => void;
  disabled?: boolean;
  error?: boolean;
  label?: string;
  "data-field-path"?: string;
  multiline?: boolean;
}
export const SecretField: React.FC<SecretFieldProps> = ({
  name,
  id,
  value,
  onUpdate,
  disabled,
  error,
  label,
  "data-field-path": dataFieldPath,
  multiline,
}) => {
  const { formatMessage } = useIntl();
  const [editingValue, setEditingValue] = useState<string | undefined>(undefined);
  const inputRef = useRef<HTMLInputElement>(null);

  const pushUpdate = useCallback(() => {
    onUpdate(editingValue ?? "");
    setEditingValue(undefined);
    if (inputRef.current) {
      inputRef.current.blur();
    }
  }, [editingValue, onUpdate]);

  const isDisabled = disabled || (!!value && editingValue === undefined);
  const handleChange = useCallback((e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>) => {
    setEditingValue(e.target.value);
  }, []);

  const handleBlur = useCallback(
    (e: React.FocusEvent<HTMLInputElement | HTMLTextAreaElement>) => {
      if (e.target.parentElement?.parentElement?.contains(e.relatedTarget)) {
        return;
      }
      if (editingValue === undefined) {
        return;
      }
      if (!value) {
        onUpdate(editingValue);
      } else {
        // If there was an existing value, don't auto-save on blur
        // This prevents a race condition where blur clears editingValue before the 'Done' button's onClick fires
        // This means a User must explicitly click 'Done' to save changes to existing secret values.
        return;
      }
      setEditingValue(undefined);
    },
    [editingValue, onUpdate, value]
  );

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent<HTMLInputElement | HTMLTextAreaElement>) => {
      // For single-line inputs, submit on Enter. For multiline, allow Enter for new lines
      if (e.key === "Enter" && !multiline) {
        e.preventDefault();
        pushUpdate();
      }
    },
    [multiline, pushUpdate]
  );

  const renderMultilineField = () => {
    const isEditing = editingValue !== undefined || !value;

    // If currently editing a multiline field, use the secretTextArea to blur values
    if (isEditing) {
      return (
        <SecretTextArea
          id={id}
          name={name}
          data-field-path={dataFieldPath}
          onChange={handleChange}
          value={editingValue ?? value}
          error={error}
          readOnly={disabled}
          disabled={disabled}
          onBlur={handleBlur}
          rows={3}
        />
      );
    }

    // When a multiline field has an existing value but is not being edited, show a static blurred value
    // since clicking on the textarea does not allow the user to edit the value.
    return (
      <TextArea
        id={id}
        name={name}
        data-field-path={dataFieldPath}
        value={formatMessage({ id: "general.maskedString" })}
        error={error}
        readOnly
        disabled
        rows={3}
      />
    );
  };

  const renderSingleLineField = () => (
    <Input
      ref={inputRef}
      id={id}
      name={name}
      data-field-path={dataFieldPath}
      onChange={handleChange}
      type="password"
      value={editingValue ?? value}
      error={error}
      readOnly={isDisabled}
      disabled={isDisabled}
      onBlur={handleBlur}
      onKeyDown={handleKeyDown}
    />
  );

  return (
    <FlexContainer direction="column" gap="sm">
      {label && <FormLabel label={label} htmlFor={id} />}
      <FlexContainer gap="sm">
        {multiline ? renderMultilineField() : renderSingleLineField()}
        {value && editingValue === undefined && (
          <Button
            type="button"
            size="sm"
            className={styles.secretButton}
            variant="secondary"
            onClick={() => setEditingValue("")}
          >
            <FormattedMessage id="form.edit" />
          </Button>
        )}
        {value && editingValue !== undefined && (
          <>
            <Button
              type="button"
              size="sm"
              className={styles.secretButton}
              variant="secondary"
              onClick={() => setEditingValue(undefined)}
            >
              <FormattedMessage id="form.cancel" />
            </Button>
            <Button
              type="button"
              size="sm"
              className={styles.secretButton}
              onClick={() => {
                onUpdate(editingValue ?? "");
                setEditingValue(undefined);
              }}
            >
              <FormattedMessage id="form.done" />
            </Button>
          </>
        )}
      </FlexContainer>
    </FlexContainer>
  );
};
