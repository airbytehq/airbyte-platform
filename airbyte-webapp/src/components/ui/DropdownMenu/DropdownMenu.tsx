import { autoUpdate, useFloating, offset, flip } from "@floating-ui/react-dom";
import { Menu, MenuButton, MenuItem, MenuItems } from "@headlessui/react";
import classNames from "classnames";
import React, { AnchorHTMLAttributes } from "react";
import { createPortal } from "react-dom";
// eslint-disable-next-line no-restricted-imports
import { Link, LinkProps } from "react-router-dom";

import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import styles from "./DropdownMenu.module.scss";
import { DropdownMenuProps, MenuItemContentProps, DropdownMenuOptionType, DropdownMenuOptionAnchorType } from "./types";

const MenuItemContent: React.FC<React.PropsWithChildren<MenuItemContentProps>> = ({ data, textSize }) => {
  return (
    <>
      {data?.icon && <span className={styles.icon}>{data.icon}</span>}
      <Text className={styles.text} size={textSize}>
        {data.displayName}
      </Text>
    </>
  );
};

export const DropdownMenu: React.FC<DropdownMenuProps> = ({
  options,
  children,
  onChange,
  placement = "bottom",
  displacement = 5,
  textSize = "lg",
  style = {},
  ...restProps
}) => {
  const { x, y, reference, floating, strategy } = useFloating({
    middleware: [offset(displacement), flip()],
    whileElementsMounted: autoUpdate,
    placement,
  });

  const anchorProps = (item: DropdownMenuOptionAnchorType) => {
    return {
      target: item.internal ? undefined : "_blank",
      rel: item.internal ? undefined : "noreferrer",
      to: item.href,
      href: item.href,
    };
  };

  const elementProps = (item: DropdownMenuOptionType, focus: boolean) => {
    return {
      onClick: () => onChange && onChange(item),
      className: classNames(styles.item, item?.className, {
        [styles.iconPositionLeft]: (item?.iconPosition === "left" && item.icon) || !item?.iconPosition,
        [styles.iconPositionRight]: item?.iconPosition === "right",
        [styles.focus]: focus,
        [styles.disabled]: item.disabled,
      }),
      disabled: item.disabled,
      "data-testid": item["data-testid"],
    } as LinkProps | AnchorHTMLAttributes<Element>;
  };

  const menuItem = (item: DropdownMenuOptionType, index: number) => (
    <MenuItem key={index} disabled={item.disabled}>
      {({ focus }) =>
        item.as === "a"
          ? React.createElement(
              item.internal ? Link : "a",
              { ...elementProps(item, focus), ...anchorProps(item) },
              <MenuItemContent data={item} textSize={textSize} />
            )
          : React.createElement(
              item.as ?? "button",
              { ...elementProps(item, focus) },
              <MenuItemContent data={item} textSize={textSize} />
            )
      }
    </MenuItem>
  );

  return (
    <Menu ref={reference} as="div" {...(restProps["data-testid"] && { "data-testid": restProps["data-testid"] })}>
      {({ open }) => (
        <>
          <MenuButton as="span">{children({ open })}</MenuButton>
          {createPortal(
            <MenuItems
              ref={floating}
              className={styles.items}
              style={{
                position: strategy,
                top: y ?? 0,
                left: x ?? 0,
                ...style,
              }}
            >
              {options.map((item, index) => {
                if (item.as === "separator") {
                  return <div role="presentation" className={styles.separator} key={index} />;
                }
                if (item.as === "div") {
                  return (
                    <div className={item.className} key={index}>
                      {item.children}
                    </div>
                  );
                }
                return item.tooltipContent != null ? (
                  <div key={index}>
                    <Tooltip control={menuItem(item, index)} placement="left">
                      {item.tooltipContent}
                    </Tooltip>
                  </div>
                ) : (
                  menuItem(item, index)
                );
              })}
            </MenuItems>,
            document.body
          )}
        </>
      )}
    </Menu>
  );
};
