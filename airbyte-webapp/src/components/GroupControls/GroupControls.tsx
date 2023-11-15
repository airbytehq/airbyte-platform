import classNames from "classnames";
import React from "react";
import { FormattedMessage } from "react-intl";

import { PropertyError } from "views/Connector/ConnectorForm/components/Property/PropertyError";

import styles from "./GroupControls.module.scss";

interface GroupControlsProps {
  label?: React.ReactNode;
  control?: React.ReactNode;
  controlClassName?: string;
  name?: string;
  error?: string;
}

const GroupControls: React.FC<React.PropsWithChildren<GroupControlsProps>> = ({
  label,
  control,
  children,
  name,
  controlClassName,
  error,
}) => {
  const hasTitle = Boolean(label || control);

  return (
    // This outer div is necessary for .content > :first-child padding to be properly applied in the case of nested GroupControls
    <div>
      <div className={classNames(styles.container, { [styles["container--title"]]: hasTitle })} data-testid={name}>
        {hasTitle && (
          <div className={styles.title}>
            <div className={styles.label}>{label}</div>
            <div className={classNames(styles.control, controlClassName)}>{control}</div>
          </div>
        )}
        <div
          className={classNames(styles.content, {
            [styles["content--error"]]: error,
            [styles["content--title"]]: hasTitle,
          })}
        >
          {children}
        </div>
      </div>
      {error && (
        <PropertyError>
          <FormattedMessage id={error} />
        </PropertyError>
      )}
    </div>
  );
};

export default GroupControls;
