import classNames from "classnames";
import React from "react";
import { FormattedMessage } from "react-intl";
import { NavLink } from "react-router-dom";

import { Badge } from "components/ui/Badge";
import { FlexContainer } from "components/ui/Flex";
import { Icon, IconProps } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import styles from "./NavItem.module.scss";
import { NotificationIndicator } from "../NotificationIndicator";

interface NavItemBaseProps extends Omit<NavItemInnerProps, "isActive"> {
  className?: string;
  activeClassName?: string;
  testId?: string;
  disabled?: boolean;
  external?: boolean;
}

interface LinkNavItemProps extends NavItemBaseProps {
  as?: "a";
  to: string;
  onClick?: undefined;
  isActive?: false;
}

interface ButtonNavItemProps extends NavItemBaseProps {
  as: "button";
  onClick?: () => void;
  to?: undefined;
  isActive?: boolean;
}

type NavItemProps = LinkNavItemProps | ButtonNavItemProps;

interface NavItemInnerProps {
  label?: React.ReactNode;
  icon: IconProps["type"];
  withNotification?: boolean;
  isActive?: boolean;
  withBadge?: "beta";
}

const NavItemInner: React.FC<NavItemInnerProps> = ({ icon, label, withNotification, isActive, withBadge }) => {
  return (
    <FlexContainer direction="row" alignItems="center" gap="md">
      <span className={styles.icon}>
        <Icon type={icon} />
      </span>
      {label && (
        <Text size="sm" color={isActive ? "darkBlue" : "grey500"} bold className={styles.label}>
          {label}
        </Text>
      )}
      {withBadge && (
        <Badge variant="blue" className={styles.badge}>
          {withBadge === "beta" && <FormattedMessage id="sidebar.beta" />}
        </Badge>
      )}
      {withNotification && <NotificationIndicator />}
    </FlexContainer>
  );
};

export const NavItem = React.forwardRef<HTMLButtonElement | null, NavItemProps>(
  (
    {
      disabled,
      label,
      icon,
      to,
      testId,
      as,
      className,
      activeClassName,
      onClick,
      withNotification = false,
      isActive,
      withBadge,
      external,
    },
    ref
  ) => {
    const menuItemStyle = (isActive?: boolean, disabled?: boolean) => {
      return classNames(
        styles.menuItem,
        className,
        {
          [styles.active]: isActive,
          [styles.disabled]: disabled,
        },
        isActive && activeClassName
      );
    };

    if (as === "button") {
      return (
        <button
          type="button"
          disabled={disabled}
          onClick={onClick}
          className={classNames(styles.menuItem, className, { [styles.active]: isActive })}
          data-testid={testId}
          ref={ref}
        >
          <NavItemInner
            label={label}
            icon={icon}
            withNotification={withNotification}
            isActive={isActive}
            withBadge={withBadge}
          />
        </button>
      );
    }

    if (disabled) {
      return (
        <div className={menuItemStyle(false, true)}>
          <NavItemInner label={label} icon={icon} />
        </div>
      );
    }

    if (external) {
      return (
        <a className={menuItemStyle(false)} href={to} target="_blank" rel="noopener noreferrer" data-testid={testId}>
          <NavItemInner label={label} icon={icon} withNotification={withNotification} withBadge={withBadge} />
        </a>
      );
    }

    return (
      <NavLink
        className={({ isActive: linkActive }) => menuItemStyle(isActive ?? linkActive)}
        to={to}
        data-testid={testId}
        aria-current={isActive === false ? false : undefined}
      >
        {({ isActive: linkActive }) => (
          <NavItemInner
            label={label}
            icon={icon}
            withNotification={withNotification}
            isActive={isActive ?? linkActive}
            withBadge={withBadge}
          />
        )}
      </NavLink>
    );
  }
);

NavItem.displayName = "NavItem";
