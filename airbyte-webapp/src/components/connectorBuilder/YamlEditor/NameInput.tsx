import classNames from "classnames";
import { useEffect, useRef, useState } from "react";
import { useController } from "react-hook-form";
import { useIntl } from "react-intl";

import { Input } from "components/ui/Input";

import styles from "./NameInput.module.scss";

export const NameInput = () => {
  const { formatMessage } = useIntl();
  const {
    field,
    fieldState: { error },
  } = useController({ name: "name" });
  const hasError = Boolean(error);
  const [showInput, setShowInput] = useState(false);
  const inputRef = useRef<HTMLInputElement | null>(null);

  useEffect(() => {
    if (showInput && inputRef.current) {
      inputRef.current.focus();
    }
  });

  if (!showInput) {
    return (
      <button
        type="button"
        className={classNames(styles.label, { [styles.invalid]: hasError })}
        onClick={() => {
          setShowInput(true);
        }}
      >
        {field.value || formatMessage({ id: "connectorBuilder.emptyName" })}
      </button>
    );
  }
  return (
    <Input
      containerClassName={styles.inputContainer}
      className={styles.input}
      {...field}
      ref={(el) => {
        inputRef.current = el;
        field.ref(el);
      }}
      onBlur={() => {
        setShowInput(false);
        field.onBlur();
      }}
      type="text"
      value={field.value ?? ""}
      error={hasError}
    />
  );
};
