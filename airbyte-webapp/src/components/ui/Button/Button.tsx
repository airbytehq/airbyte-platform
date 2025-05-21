import classNames from "classnames";
import React from "react";

import { Icon } from "components/ui/Icon";

import styles from "./Button.module.scss";
import { ButtonProps } from "./types";

export const Button = React.forwardRef<HTMLButtonElement, ButtonProps>((props, ref) => {
  const {
    full = false,
    size = "xs",
    variant = "primary",
    children,
    className,
    isLoading,
    width,
    disabled,
    icon,
    iconSize,
    iconColor,
    iconClassName,
    iconPosition = "left",
    ...buttonProps
  } = props;

  const buttonStyles = {
    [styles["button--full-width"]]: full,
    [styles["button--loading"]]: isLoading,
    [styles["button--size-sm"]]: size === "sm",
    [styles["button--size-xs"]]: size === "xs",
    [styles["button--danger"]]: variant === "danger",
    [styles["button--clear"]]: variant === "clear",
    [styles["button--clear-danger"]]: variant === "clearDanger",
    [styles["button--magic"]]: variant === "magic",
    [styles["button--primary"]]: variant === "primary",
    [styles["button--secondary"]]: variant === "secondary",
    [styles["button--clear-dark"]]: variant === "clearDark",
    [styles["button--primary-dark"]]: variant === "primaryDark",
    [styles["button--secondary-dark"]]: variant === "secondaryDark",
    [styles["button--link"]]: variant === "link",
  };

  const widthStyle: React.CSSProperties = width ? { width: `${width}px` } : {};

  return (
    <button
      ref={ref}
      style={widthStyle}
      className={classNames(styles.button, buttonStyles, className)}
      disabled={disabled || isLoading}
      {...buttonProps}
    >
      {isLoading && (
        <Icon type="loading" className={classNames(styles.button__icon, styles["button__icon--loading"])} />
      )}
      {icon && iconPosition === "left" && (
        <Icon
          type={icon}
          size={iconSize}
          color={iconColor}
          className={classNames(
            styles.button__icon,
            styles["button__icon--regular"],
            styles["button__icon--left"],
            {
              [styles["button__icon--with-label"]]: Boolean(children),
            },
            iconClassName
          )}
        />
      )}
      <span className={styles.button__children}>{children}</span>
      {icon && iconPosition === "right" && (
        <Icon
          type={icon}
          size={iconSize}
          color={iconColor}
          className={classNames(
            styles.button__icon,
            styles["button__icon--regular"],
            styles["button__icon--right"],
            {
              [styles["button__icon--with-label"]]: Boolean(children),
            },
            iconClassName
          )}
        />
      )}
    </button>
  );
});
Button.displayName = "Button";
