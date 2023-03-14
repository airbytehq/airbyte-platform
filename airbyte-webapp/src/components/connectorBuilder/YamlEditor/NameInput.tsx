import classNames from "classnames";
import { useField } from "formik";
import { useEffect, useRef, useState } from "react";
import { useIntl } from "react-intl";

import { Input } from "components/ui/Input";

import styles from "./NameInput.module.scss";

export const NameInput = () => {
  const { formatMessage } = useIntl();
  const [field, meta] = useField<string>("global.connectorName");
  const hasError = Boolean(meta.error);
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
      ref={(el) => (inputRef.current = el)}
      {...field}
      onBlur={(e) => {
        setShowInput(false);
        field.onBlur(e);
      }}
      type="text"
      value={field.value ?? ""}
      error={hasError}
    />
  );
};
