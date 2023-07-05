import classNames from "classnames";
import React from "react";

import { Text } from "components/ui/Text";

import styles from "./Card.module.scss";
import { Heading } from "../Heading";
import { InfoTooltip } from "../Tooltip";

export interface CardProps {
  title?: React.ReactNode;
  description?: React.ReactNode;
  className?: string;
  fullWidth?: boolean;
  lightPadding?: boolean;
  withPadding?: boolean;
  roundedBottom?: boolean;
  inset?: boolean;
}

export const Card: React.FC<React.PropsWithChildren<CardProps>> = ({
  children,
  title,
  description,
  className,
  fullWidth,
  lightPadding,
  withPadding,
  roundedBottom,
  inset = false,
  ...restProps
}) => (
  <div
    className={classNames(className, styles.container, {
      [styles.fullWidth]: fullWidth,
      [styles.withPadding]: withPadding,
      [styles.inset]: inset,
    })}
    {...restProps}
  >
    {title ? (
      <div
        className={classNames(styles.header, {
          [styles.lightPadding]: lightPadding || !children,
          [styles.roundedBottom]: roundedBottom,
          [styles.withDescription]: description,
        })}
      >
        <Heading as="h5" size="sm">
          {title}
        </Heading>
        {description && (
          <InfoTooltip>
            <Text className={styles.infoTooltip} size="sm">
              {description}
            </Text>
          </InfoTooltip>
        )}
      </div>
    ) : null}
    {children}
  </div>
);
