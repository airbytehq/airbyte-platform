import classNames from "classnames";
import { useCallback, useMemo, useRef } from "react";
import { FormattedMessage } from "react-intl";
import { useToggle, useUpdateEffect } from "react-use";

import { Icon } from "components/ui/Icon";

import styles from "./SecretTextArea.module.scss";
import { FileUpload } from "../FileUpload/FileUpload";
import { FlexContainer } from "../Flex";
import { TextInputContainer, TextInputContainerProps } from "../TextInputContainer";

interface SecretTextAreaProps
  extends Omit<TextInputContainerProps, "onFocus" | "onBlur">,
    React.TextareaHTMLAttributes<HTMLTextAreaElement> {
  onUpload?: (value: string) => void;
  hiddenMessage?: string;
  hiddenWhenEmpty?: boolean;
  leftJustified?: boolean;
}

export const SecretTextArea: React.FC<SecretTextAreaProps> = ({
  name,
  disabled,
  value,
  onMouseUp,
  onBlur,
  error,
  light,
  onUpload,
  hiddenMessage,
  hiddenWhenEmpty,
  leftJustified,
  ...textAreaProps
}) => {
  const shouldReveal = useMemo(
    () => hiddenWhenEmpty || (!!value && String(value).trim().length > 0),
    [value, hiddenWhenEmpty]
  );
  const [isContentVisible, toggleIsContentVisible] = useToggle(!shouldReveal);
  const textAreaRef = useRef<HTMLTextAreaElement | null>(null);
  const textAreaHeightRef = useRef<number>((textAreaProps.rows ?? 1) * 20 + 14);

  const handleUpload = (uploadedValue: string) => {
    onUpload?.(uploadedValue);
    toggleIsContentVisible(true);
    focusTextArea(uploadedValue.length);
  };

  const focusTextArea = useCallback((cursorLocation: number) => {
    if (textAreaRef.current) {
      textAreaRef.current.focus();
      textAreaRef.current.setSelectionRange(cursorLocation, cursorLocation);
    }
  }, []);

  useUpdateEffect(() => {
    if (isContentVisible) {
      focusTextArea(value ? String(value).length : 0);
    }
  }, [isContentVisible]);

  return (
    <FlexContainer className={styles.container}>
      <TextInputContainer disabled={disabled} error={error} light={light}>
        {isContentVisible ? (
          <textarea
            spellCheck={false}
            {...textAreaProps}
            className={classNames(styles.textarea, "fs-exclude", textAreaProps.className)}
            name={name}
            disabled={disabled}
            ref={textAreaRef}
            onMouseUp={(event) => {
              textAreaHeightRef.current = textAreaRef.current?.offsetHeight ?? textAreaHeightRef.current;
              onMouseUp?.(event);
            }}
            onBlur={(event) => {
              textAreaHeightRef.current = textAreaRef.current?.offsetHeight ?? textAreaHeightRef.current;
              if (shouldReveal) {
                toggleIsContentVisible();
              }
              onBlur?.(event);
            }}
            style={{ height: textAreaHeightRef.current }}
            value={value}
            data-testid="secretTextArea-textarea"
          />
        ) : (
          <>
            <button
              type="button"
              className={classNames(styles.toggleVisibilityButton, { [styles.leftJustified]: leftJustified })}
              onClick={toggleIsContentVisible}
              style={{
                height: textAreaHeightRef.current,
              }}
              disabled={disabled}
              data-testid="secretTextArea-visibilityButton"
            >
              <Icon type="eye" className={styles.icon} />
              {hiddenMessage || <FormattedMessage id="ui.secretTextArea.hidden" />}
            </button>
            <input
              type="password"
              name={name}
              disabled
              value={value}
              className={styles.passwordInput}
              readOnly
              aria-hidden
              data-testid="secretTextArea-input"
            />
          </>
        )}
      </TextInputContainer>
      {onUpload && <FileUpload onUpload={handleUpload} />}
    </FlexContainer>
  );
};
