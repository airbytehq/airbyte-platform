import classNames from "classnames";
import React from "react";

import { Icon } from "components/ui/Icon";

import styles from "./Button.module.scss";
import { ButtonProps } from "./types";

export const Button = React.forwardRef<HTMLButtonElement, ButtonProps>((props, ref) => {
  const {
    full = false,
    size = "xs",
    iconPosition = "left",
    variant = "primary",
    children,
    className,
    icon,
    isLoading,
    width,
    disabled,
    ...buttonProps
  } = props;

  const buttonStyles = {
    [styles.full]: full,
    [styles.isLoading]: isLoading,
    [styles.sizeL]: size === "lg",
    [styles.sizeS]: size === "sm",
    [styles.sizeXS]: size === "xs",
    [styles.typeDanger]: variant === "danger",
    [styles.typeClear]: variant === "clear",
    [styles.typeLight]: variant === "light",
    [styles.typePrimary]: variant === "primary",
    [styles.typeSecondary]: variant === "secondary",
    [styles.typeDark]: variant === "dark",
    [styles.link]: variant === "link",
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
      {isLoading && <Icon type="loading" className={classNames(styles.buttonIcon, styles.loadingIcon)} />}
      {icon &&
        iconPosition === "left" &&
        React.cloneElement(icon, {
          className: classNames(styles.buttonIcon, {
            [styles.positionLeft]: true,
            [styles.isRegularIcon]: true,
            [styles.withLabel]: Boolean(children),
          }),
        })}
      <span className={styles.childrenContainer}>{children}</span>
      {icon &&
        iconPosition === "right" &&
        React.cloneElement(icon, {
          className: classNames(styles.buttonIcon, {
            [styles.positionRight]: true,
            [styles.isRegularIcon]: true,
            [styles.withLabel]: Boolean(children),
          }),
        })}
    </button>
  );
});
Button.displayName = "Button";
