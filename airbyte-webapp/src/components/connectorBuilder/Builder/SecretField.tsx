import { useCallback, useRef, useState } from "react";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Input } from "components/ui/Input";

import styles from "./SecretField.module.scss";

interface SecretFieldProps {
  name: string;
  value: string;
  onUpdate: (value: string) => void;
  disabled?: boolean;
  error?: boolean;
}
export const SecretField: React.FC<SecretFieldProps> = ({ name, value, onUpdate, disabled, error }) => {
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
  return (
    <FlexContainer gap="sm">
      <Input
        ref={inputRef}
        name={name}
        onChange={(e) => {
          setEditingValue(e.target.value);
        }}
        type="password"
        value={editingValue ?? value}
        error={error}
        readOnly={isDisabled}
        disabled={isDisabled}
        onBlur={(e) => {
          if (e.target.parentElement?.parentElement?.contains(e.relatedTarget)) {
            return;
          }
          if (editingValue === undefined) {
            return;
          }
          if (!value) {
            onUpdate(editingValue);
          }
          setEditingValue(undefined);
        }}
        onKeyDown={(e) => {
          if (e.key === "Enter") {
            e.preventDefault();
            pushUpdate();
          }
        }}
      />
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
  );
};
