import classNames from "classnames";
import React, { ReactNode, useCallback, useImperativeHandle, useRef, useState } from "react";
import { useIntl } from "react-intl";
import { useToggle } from "react-use";

import styles from "./Input.module.scss";
import { Button } from "../Button";

export interface InputProps extends React.InputHTMLAttributes<HTMLInputElement> {
  error?: boolean;
  light?: boolean;
  inline?: boolean;
  containerClassName?: string;
  adornment?: ReactNode;
  "data-testid"?: string;
}

export const Input = React.forwardRef<HTMLInputElement, InputProps>(
  ({ light, error, inline, containerClassName, adornment, "data-testid": testId, ...props }, ref) => {
    const { formatMessage } = useIntl();

    const [inputRef, setInputRef] = useState<HTMLInputElement | null>(null);
    const buttonRef = useRef<HTMLButtonElement | null>(null);
    const inputSelectionStartRef = useRef<number | null>(null);

    // Necessary to bind a ref passed from the parent in to our internal inputRef
    useImperativeHandle(ref, () => inputRef as HTMLInputElement, [inputRef]);

    const [isContentVisible, toggleIsContentVisible] = useToggle(false);
    const [focused, setFocused] = useState(false);

    const isPassword = props.type === "password";
    const isVisibilityButtonVisible = isPassword && !props.disabled;
    const type = isPassword ? (isContentVisible ? "text" : "password") : props.type;

    const focusOnInputElement = useCallback(() => {
      if (!inputRef) {
        return;
      }

      const element = inputRef;
      const selectionStart = inputSelectionStartRef.current ?? inputRef.value.length;

      element.focus();

      if (selectionStart) {
        // Update input cursor position to where it was before
        window.setTimeout(() => {
          element.setSelectionRange(selectionStart, selectionStart);
        }, 0);
      }
    }, [inputRef]);

    const onContainerFocus: React.FocusEventHandler<HTMLDivElement> = () => {
      setFocused(true);
    };

    const onContainerBlur: React.FocusEventHandler<HTMLDivElement> = (event) => {
      if (isVisibilityButtonVisible && event.target === inputRef) {
        // Save the previous selection
        inputSelectionStartRef.current = inputRef.selectionStart;
      }

      setFocused(false);

      if (isPassword) {
        window.setTimeout(() => {
          if (document.activeElement !== inputRef && document.activeElement !== buttonRef.current) {
            toggleIsContentVisible(false);
            inputSelectionStartRef.current = null;
          }
        }, 0);
      }
    };

    return (
      <div
        className={classNames(containerClassName, styles.container, {
          [styles.disabled]: props.disabled,
          [styles.readOnly]: props.readOnly,
          [styles.focused]: focused,
          [styles.light]: light,
          [styles.inline]: inline,
          [styles.error]: error,
        })}
        data-testid="input-container"
        onFocus={onContainerFocus}
        onBlur={onContainerBlur}
      >
        <input
          aria-invalid={error}
          data-testid={testId ?? "input"}
          {...props}
          ref={setInputRef}
          type={type}
          className={classNames(
            styles.input,
            {
              [styles.disabled]: props.disabled,
              [styles.readOnly]: props.readOnly,
              [styles.password]: isPassword,
              "fs-exclude": isPassword,
            },
            props.className
          )}
        />
        {adornment}
        {isVisibilityButtonVisible ? (
          <Button
            ref={buttonRef}
            className={styles.visibilityButton}
            onClick={() => {
              toggleIsContentVisible();
              focusOnInputElement();
            }}
            tabIndex={-1}
            type="button"
            variant="clear"
            aria-label={formatMessage({
              id: `ui.input.${isContentVisible ? "hide" : "show"}Password`,
            })}
            data-testid="toggle-password-visibility-button"
            icon={isContentVisible ? "eyeSlash" : "eye"}
          />
        ) : null}
      </div>
    );
  }
);
Input.displayName = "Input";
