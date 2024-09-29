import classNames from "classnames";

import styles from "./DropdownButton.module.scss";
import { Button, ButtonProps } from "../Button";
import { DropdownMenu, DropdownMenuOptionType, DropdownMenuProps } from "../DropdownMenu";
import { FlexContainer } from "../Flex";

export interface DropdownButtonProps extends ButtonProps {
  dropdown: Omit<DropdownMenuProps, "children" | "onChange" | "data-testid"> & {
    onSelect: (data: DropdownMenuOptionType) => void;
  };
}

export const DropdownButton: React.FC<React.PropsWithChildren<DropdownButtonProps>> = ({
  children,
  dropdown,
  ...buttonProps
}) => {
  // explicitly pull out the props that should not be passed to the dropdown caret button
  const {
    icon,
    iconSize,
    iconColor,
    iconClassName,
    iconPosition,
    onClick,
    full,
    isLoading,
    width,
    "data-testid": dataTestId,
    ...dropdownCaretButtonProps
  } = buttonProps;

  const separatorStyles = {
    [styles.primarySeparator]: !buttonProps.variant || buttonProps.variant === "primary",
    [styles.dangerSeparator]: buttonProps.variant && buttonProps.variant === "danger",
    [styles.primaryDarkSeparator]: buttonProps.variant && buttonProps.variant === "primaryDark",
    [styles.secondarySeparator]:
      buttonProps.variant && ["secondary", "light", "clear", "clearDark", "link"].includes(buttonProps.variant),
  };

  return (
    <FlexContainer direction="row" gap="none" className={styles.container}>
      <Button {...buttonProps} className={classNames(styles.mainButton, separatorStyles)}>
        {children}
      </Button>
      <DropdownMenu {...dropdown} placement={dropdown.placement ?? "bottom-end"} onChange={dropdown.onSelect}>
        {() => (
          <Button
            icon="caretDown"
            iconSize="sm"
            className={classNames(styles.dropdownButton, { [styles.noPointerEvents]: isLoading })}
            data-testid={dataTestId ? `${dataTestId}_dropdown-caret` : undefined}
            {...dropdownCaretButtonProps}
          />
        )}
      </DropdownMenu>
    </FlexContainer>
  );
};
