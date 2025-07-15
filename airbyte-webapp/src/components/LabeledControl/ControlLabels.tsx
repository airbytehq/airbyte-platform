import className from "classnames";
import React from "react";
import { FormattedMessage } from "react-intl";

import Label from "components/Label";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";
import { InfoTooltip } from "components/ui/Tooltip";

import styles from "./ControlLabels.module.scss";

export interface ControlLabelsProps {
  className?: string;
  headerClassName?: string;
  error?: boolean;
  success?: boolean;
  nextLine?: boolean;
  message?: React.ReactNode;
  label?: React.ReactNode;
  infoTooltipContent?: React.ReactNode;
  optional?: boolean;
  htmlFor?: string;
  format?: React.ReactNode;
  labelAction?: React.ReactNode;
}

const ControlLabels = React.forwardRef<HTMLDivElement, React.PropsWithChildren<ControlLabelsProps>>((props, ref) => (
  <div ref={ref} className={className(styles.controlContainer, props.className)}>
    {(props.label || props.labelAction) && (
      <FlexContainer gap="sm" alignItems="center" className={className(styles.headerContainer, props.headerClassName)}>
        <Label
          error={props.error}
          success={props.success}
          message={props.message}
          nextLine={props.nextLine}
          htmlFor={props.htmlFor}
          endBlock={props.format}
        >
          <FlexContainer gap="none" alignItems="center">
            {props.label}
            {props.infoTooltipContent && (
              <InfoTooltip className={styles.tooltip} placement="top-start">
                {props.infoTooltipContent}
              </InfoTooltip>
            )}
            {props.optional && (
              <Text size="sm" className={styles.optionalText}>
                <FormattedMessage id="form.optional" />
              </Text>
            )}
          </FlexContainer>
        </Label>
        {props.labelAction && props.labelAction}
      </FlexContainer>
    )}
    {props.children}
  </div>
));

export { ControlLabels };
ControlLabels.displayName = "ControlLabels";
