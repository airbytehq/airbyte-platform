import classNames from "classnames";
import React from "react";

import { FlexContainer, FlexItem } from "components/ui/Flex";

import styles from "./Label.module.scss";

interface IProps {
  error?: boolean;
  nextLine?: boolean;
  success?: boolean;
  message?: string | React.ReactNode;
  className?: string;
  htmlFor?: string;
  endBlock?: React.ReactNode;
}

const Label: React.FC<React.PropsWithChildren<IProps>> = (props) => (
  <label className={classNames(props.className, styles.label)} htmlFor={props.htmlFor}>
    <FlexContainer gap="sm" direction={props.children && props.nextLine ? "column" : "row"}>
      {props.children}
      <FlexItem grow>
        {props.message && (
          <span>
            {props.children && !props.nextLine ? " - " : null}
            <span
              className={classNames(styles.message, { [styles.error]: props.error, [styles.success]: props.success })}
            >
              {props.message}
            </span>
          </span>
        )}
      </FlexItem>
      {props.endBlock}
    </FlexContainer>
  </label>
);

export default Label;
