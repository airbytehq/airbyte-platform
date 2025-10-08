import React, { useEffect, useRef, useState } from "react";
import { useController, useFormState } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { Input } from "components/ui/Input";
import { SecretTextArea } from "components/ui/SecretTextArea";
import { TextArea } from "components/ui/TextArea";

import styles from "./SecretConfirmationControl.module.scss";

interface SecretConfirmationControlProps {
  showButtons?: boolean;
  name: string;
  multiline: boolean;
  disabled?: boolean;
  error?: boolean;
  onFocus?: () => void;
  onChange: (e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>) => void;
  onBlur?: (value: string) => void;
}

const SecretConfirmationControl: React.FC<SecretConfirmationControlProps> = ({
  showButtons,
  disabled,
  multiline,
  name,
  error,
  onChange,
  onFocus,
  onBlur,
}) => {
  const { field } = useController({
    name,
  });
  const { isDirty: dirty, touchedFields } = useFormState({ name });
  const touched = Object.keys(touchedFields).length > 0;
  const [previousValue, setPreviousValue] = useState<unknown>(undefined);
  const isEditInProgress = Boolean(previousValue);
  const controlRef = useRef<HTMLInputElement>(null);

  const renderMultilineField = () => {
    const isEditing = isEditInProgress || !field.value;

    if (isEditing) {
      return (
        <SecretTextArea
          {...field}
          onChange={onChange}
          autoComplete="off"
          value={field.value ?? ""}
          rows={3}
          error={error}
          // eslint-disable-next-line jsx-a11y/no-autofocus
          autoFocus={showButtons && isEditInProgress}
          disabled={disabled}
          onUpload={(val) => field.onChange(val)}
          onFocus={onFocus}
          onBlur={() => onBlur?.(field.value)}
        />
      );
    }

    return (
      <TextArea
        {...field}
        autoComplete="off"
        value="***"
        rows={3}
        error={error}
        disabled={disabled || showButtons}
        readOnly
        onFocus={onFocus}
      />
    );
  };

  const renderSingleLineField = () => (
    <Input
      {...field}
      onChange={onChange}
      autoComplete="off"
      value={field.value ?? ""}
      type="password"
      error={error}
      ref={controlRef}
      disabled={(showButtons && !isEditInProgress) || disabled}
      onFocus={onFocus}
      onBlur={() => onBlur?.(field.value)}
    />
  );

  const component = multiline ? renderMultilineField() : renderSingleLineField();

  useEffect(() => {
    if (!dirty && !touched && previousValue) {
      setPreviousValue(undefined);
    }
  }, [dirty, previousValue, touched]);

  if (!showButtons) {
    return <>{component}</>;
  }

  const handleStartEdit = () => {
    // For single-line password inputs, manually enable and focus the field
    // For multiline SecretTextArea, the autoFocus prop handles this automatically
    if (!multiline && controlRef.current) {
      controlRef.current?.removeAttribute?.("disabled");
      controlRef.current?.focus?.();
    }
    field.onChange("");
    setPreviousValue(field.value);
  };

  const onDone = () => {
    setPreviousValue(undefined);
  };

  const onCancel = () => {
    if (previousValue) {
      field.onChange(previousValue);
    }
    setPreviousValue(undefined);
  };

  return (
    <div className={styles.container}>
      {component}
      {isEditInProgress ? (
        <>
          <Button size="sm" onClick={onCancel} type="button" variant="secondary" disabled={disabled}>
            <FormattedMessage id="form.cancel" />
          </Button>
          <Button size="sm" onClick={onDone} type="button" disabled={disabled}>
            <FormattedMessage id="form.done" />
          </Button>
        </>
      ) : (
        <Button
          size="sm"
          onClick={handleStartEdit}
          data-testid="edit-secret"
          type="button"
          variant="secondary"
          disabled={disabled}
        >
          <FormattedMessage id="form.edit" />
        </Button>
      )}
    </div>
  );
};

export default SecretConfirmationControl;
