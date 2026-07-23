import classNames from "classnames";
import { useEffect, useRef, useState } from "react";
import { useController } from "react-hook-form";
import { useIntl } from "react-intl";

import { Input } from "components/ui/Input";

import styles from "./NameInput.module.scss";

interface NameInputProps {
  className?: string;
  size: "sm" | "md";
  showBorder?: boolean;
}

export const NameInput: React.FC<NameInputProps> = ({ className, size, showBorder = false }) => {
  const { formatMessage } = useIntl();
  const {
    field,
    fieldState: { error },
  } = useController({ name: "name" });
  const hasError = Boolean(error);
  const [showInput, setShowInput] = useState(false);
  const inputRef = useRef<HTMLInputElement | null>(null);

  const sizeStyles = {
    [styles.sizeS]: size === "sm",
    [styles.sizeM]: size === "md",
  };

  useEffect(() => {
    if (showInput && inputRef.current) {
      inputRef.current.focus();
    }
  });

  if (!showInput) {
    return (
      <button
        type="button"
        className={classNames(className, styles.label, {
          ...sizeStyles,
          [styles.invalid]: hasError,
          [styles.showBorder]: showBorder,
        })}
        onClick={() => {
          setShowInput(true);
        }}
        data-testid="connector-name-label"
      >
        {field.value || formatMessage({ id: "connectorBuilder.emptyName" })}
      </button>
    );
  }
  return (
    <Input
      containerClassName={classNames(className, styles.inputContainer)}
      className={classNames(styles.input, sizeStyles)}
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
      data-testid="connector-name-input"
    />
  );
};
