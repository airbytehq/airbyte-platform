import React from "react";

import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";

import { Switch, SwitchProps } from "./Switch";
import styles from "./SwitchWithLock.module.scss";

export interface SwitchWithLockProps extends SwitchProps {
  showLock?: boolean;
}

/**
 * Switch component to show a lock icon when the connection is prevented from being enabled
 */
export const SwitchWithLock: React.FC<SwitchWithLockProps> = ({ showLock, ...switchProps }) => (
  <div className={styles.container}>
    <Switch {...switchProps} />
    {showLock && (
      <FlexContainer className={styles.lockIcon} alignItems="center" justifyContent="center">
        <Icon type="lock" size="xs" />
      </FlexContainer>
    )}
  </div>
);
