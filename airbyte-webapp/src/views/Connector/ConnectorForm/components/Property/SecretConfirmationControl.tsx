import React, { useEffect, useRef, useState } from "react";
import { useController, useFormState } from "react-hook-form";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { Input } from "components/ui/Input";
import { SecretTextArea } from "components/ui/SecretTextArea";

import styles from "./SecretConfirmationControl.module.scss";

interface SecretConfirmationControlProps {
  showButtons?: boolean;
  name: string;
  multiline: boolean;
  disabled?: boolean;
  error?: boolean;
  onFocus?: () => void;
  onChange: (e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>) => void;
}

const SecretConfirmationControl: React.FC<SecretConfirmationControlProps> = ({
  showButtons,
  disabled,
  multiline,
  name,
  error,
  onChange,
  onFocus,
}) => {
  const { field } = useController({
    name,
  });
  const { isDirty: dirty, touchedFields } = useFormState({ name });
  const touched = Object.keys(touchedFields).length > 0;
  const [previousValue, setPreviousValue] = useState<unknown>(undefined);
  const isEditInProgress = Boolean(previousValue);
  const controlRef = useRef<HTMLInputElement>(null);

  const component =
    multiline && (isEditInProgress || !showButtons) ? (
      <SecretTextArea
        {...field}
        onChange={onChange}
        autoComplete="off"
        value={field.value ?? ""}
        rows={3}
        error={error}
        // eslint-disable-next-line jsx-a11y/no-autofocus
        autoFocus={showButtons && isEditInProgress}
        disabled={(showButtons && !isEditInProgress) || disabled}
        onUpload={(val) => field.onChange(val)}
        onFocus={onFocus}
      />
    ) : (
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
      />
    );

  useEffect(() => {
    if (!dirty && !touched && previousValue) {
      setPreviousValue(undefined);
    }
  }, [dirty, previousValue, touched]);

  if (!showButtons) {
    return <>{component}</>;
  }

  const handleStartEdit = () => {
    if (controlRef && controlRef.current) {
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
