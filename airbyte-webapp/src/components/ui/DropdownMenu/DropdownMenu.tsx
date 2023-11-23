import { autoUpdate, useFloating, offset } from "@floating-ui/react-dom";
import { Menu } from "@headlessui/react";
import classNames from "classnames";
import React, { AnchorHTMLAttributes } from "react";
// eslint-disable-next-line no-restricted-imports
import { Link, LinkProps } from "react-router-dom";

import { Text } from "components/ui/Text";

import styles from "./DropdownMenu.module.scss";
import { DropdownMenuProps, MenuItemContentProps, DropdownMenuOptionType, DropdownMenuOptionAnchorType } from "./types";

const MenuItemContent: React.FC<React.PropsWithChildren<MenuItemContentProps>> = ({ data }) => {
  return (
    <>
      {data?.icon && <span className={styles.icon}>{data.icon}</span>}
      <Text className={styles.text} size="lg">
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
  ...restProps
}) => {
  const { x, y, reference, floating, strategy } = useFloating({
    middleware: [offset(displacement)],
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

  const elementProps = (item: DropdownMenuOptionType, active: boolean) => {
    return {
      title: item.displayName,
      onClick: () => onChange && onChange(item),
      className: classNames(styles.item, item?.className, {
        [styles.iconPositionLeft]: (item?.iconPosition === "left" && item.icon) || !item?.iconPosition,
        [styles.iconPositionRight]: item?.iconPosition === "right",
        [styles.active]: active,
        [styles.disabled]: item.disabled,
      }),
      disabled: item.disabled,
      "data-testid": item["data-testid"],
    } as LinkProps | AnchorHTMLAttributes<Element>;
  };

  return (
    <Menu ref={reference} as="div" {...(restProps["data-testid"] && { "data-testid": restProps["data-testid"] })}>
      {({ open }) => (
        <>
          <Menu.Button as={React.Fragment}>{children({ open })}</Menu.Button>
          <Menu.Items
            ref={floating}
            className={styles.items}
            style={{
              position: strategy,
              top: y ?? 0,
              left: x ?? 0,
            }}
          >
            {options.map((item, index) => {
              if (item.as === "separator") {
                return <div role="presentation" className={styles.separator} key={index} />;
              }
              return (
                <Menu.Item key={index} disabled={item.disabled}>
                  {({ active }) =>
                    item.as === "a"
                      ? React.createElement(
                          item.internal ? Link : "a",
                          { ...elementProps(item, active), ...anchorProps(item) },
                          <MenuItemContent data={item} />
                        )
                      : React.createElement(
                          item.as ?? "button",
                          { ...elementProps(item, active) },
                          <MenuItemContent data={item} />
                        )
                  }
                </Menu.Item>
              );
            })}
          </Menu.Items>
        </>
      )}
    </Menu>
  );
};
