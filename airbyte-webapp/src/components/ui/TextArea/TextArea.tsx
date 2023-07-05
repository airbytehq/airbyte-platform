import classNames from "classnames";
import React from "react";

import styles from "./TextArea.module.scss";
import { FileUpload } from "../FileUpload/FileUpload";
import { FlexContainer } from "../Flex";
import { TextInputContainer } from "../TextInputContainer";

export interface TextAreaProps extends React.TextareaHTMLAttributes<HTMLTextAreaElement> {
  error?: boolean;
  light?: boolean;
  onUpload?: (value: string) => void;
}

export const TextArea = React.forwardRef<HTMLTextAreaElement, React.PropsWithChildren<TextAreaProps>>(
  ({ error, light, onUpload, disabled, children, className, ...textAreaProps }, ref) => {
    return (
      <FlexContainer className={styles.container}>
        <TextInputContainer disabled={disabled} error={error} light={light}>
          <textarea
            {...textAreaProps}
            ref={ref}
            className={classNames(
              styles.textarea,
              {
                [styles.error]: error,
                [styles.light]: light,
              },
              className
            )}
          >
            {children}
          </textarea>
        </TextInputContainer>
        {onUpload && <FileUpload onUpload={onUpload} />}
      </FlexContainer>
    );
  }
);
TextArea.displayName = "TextArea";
