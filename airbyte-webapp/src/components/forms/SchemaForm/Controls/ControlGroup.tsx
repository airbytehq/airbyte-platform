import classNames from "classnames";
import isEmpty from "lodash/isEmpty";
import React from "react";
import { FieldError } from "react-hook-form";
import { useIntl } from "react-intl";

import { CheckBox } from "components/ui/CheckBox";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { NON_I18N_ERROR_TYPE } from "core/utils/form";

import styles from "./ControlGroup.module.scss";
import { FormLabel } from "../../FormControl";

interface ControlGroupProps {
  path: string;
  title?: string;
  tooltip?: React.ReactNode;
  optional?: boolean;
  control?: React.ReactNode;
  header?: React.ReactNode;
  error?: FieldError;
  toggleConfig?: {
    isEnabled?: boolean;
    onToggle: (newEnabledState: boolean) => void;
  };
}

export const ControlGroup = React.forwardRef<HTMLDivElement, React.PropsWithChildren<ControlGroupProps>>(
  ({ title, path, tooltip, optional, control, header, error, toggleConfig, children }, ref) => {
    const { formatMessage } = useIntl();

    const isDisabled = toggleConfig && toggleConfig.isEnabled === false;
    const hasTitleBar = Boolean(title || (control && !isDisabled) || header);
    const hasNoContent = isDisabled || isEmpty(children);

    return (
      // This outer div is necessary for .content > :first-child padding to be properly applied in the case of nested GroupControls
      <div ref={ref} className={styles.outer}>
        <div
          className={classNames(styles.container, {
            [styles["container--noContent"]]: hasNoContent,
          })}
        >
          <div
            className={classNames(styles.content, {
              [styles["content--error"]]: error,
              [styles["content--title"]]: hasTitleBar,
              [styles["content--borderless"]]: isDisabled,
            })}
          >
            {isDisabled ? null : children}
          </div>
          <div className={styles.titleBar}>
            {title && (
              <FlexContainer
                alignItems="center"
                className={classNames(styles.title, { [styles["title--pointer"]]: !!toggleConfig })}
              >
                {toggleConfig && (
                  <CheckBox
                    id={path}
                    checked={toggleConfig.isEnabled}
                    onChange={(event) => {
                      if (event.target.checked) {
                        toggleConfig.onToggle(true);
                      } else {
                        toggleConfig.onToggle(false);
                      }
                    }}
                  />
                )}
                <FormLabel label={title} labelTooltip={tooltip} htmlFor={path} optional={optional} />
                {header}
              </FlexContainer>
            )}
            <FlexContainer alignItems="center" justifyContent="flex-end" gap="xs">
              {control && !isDisabled && <div className={styles.control}>{control}</div>}
            </FlexContainer>
          </div>
        </div>
        {error && (
          <Text color="red" size="xs" className={styles.error}>
            {error.type === NON_I18N_ERROR_TYPE ? error.message : formatMessage({ id: error.message })}
          </Text>
        )}
      </div>
    );
  }
);
ControlGroup.displayName = "ControlGroup";
