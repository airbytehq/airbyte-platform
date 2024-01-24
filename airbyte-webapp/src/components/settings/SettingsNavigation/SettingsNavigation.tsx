import classNames from "classnames";
import React, { PropsWithChildren } from "react";
import { NavLink } from "react-router-dom";

import { Box } from "components/ui/Box";
import { FlexContainer } from "components/ui/Flex";
import { Icon, IconType } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import styles from "./SettingsNavigation.module.scss";

export const SettingsNavigation: React.FC<PropsWithChildren> = ({ children }) => {
  return (
    <FlexContainer direction="column" gap="md" className={styles.settingsNavigation}>
      {children}
    </FlexContainer>
  );
};

interface SettingsNavigationBlockProps {
  title: string;
}

export const SettingsNavigationBlock: React.FC<PropsWithChildren<SettingsNavigationBlockProps>> = ({
  title,
  children,
}) => {
  return (
    <Box pb="lg">
      <Box pb="sm">
        <Text size="sm" bold color="grey" className={styles.settingsNavigation__blockTitle}>
          {title}
        </Text>
      </Box>
      {children}
    </Box>
  );
};

interface SettingsLinkProps {
  name: string | React.ReactNode;
  iconType: IconType;
  count?: number;
  id?: string;
  to: string;
}

export const SettingsLink: React.FC<SettingsLinkProps> = ({ count = 0, name, id, to, iconType }) => {
  return (
    <NavLink
      end
      to={to}
      data-test-id={id}
      className={({ isActive }) =>
        classNames(styles.settingsNavigation__link, {
          [styles["settingsNavigation__link--active"]]: isActive,
        })
      }
    >
      <Icon type={iconType} size="sm" className={styles.settingsNavigation__linkIcon} />
      {name}
      {count > 0 && <div className={styles.settingsNavigation__counter}>{count}</div>}
    </NavLink>
  );
};

interface SettingsButtonProps {
  name: string | React.ReactNode;
  id?: string;
  onClick: () => void;
  iconType: IconType;
}

export const SettingsButton: React.FC<SettingsButtonProps> = ({ name, id, onClick, iconType }) => {
  return (
    <button onClick={onClick} className={styles.settingsNavigation__button} data-test-id={id}>
      <Icon type={iconType} size="sm" className={styles.settingsNavigation__buttonIcon} />
      {name}
    </button>
  );
};
