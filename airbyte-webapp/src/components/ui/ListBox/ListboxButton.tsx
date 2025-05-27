import { ListboxButton as HeadlessUIListboxButton } from "@headlessui/react";
import classNames from "classnames";
import React, { ComponentType } from "react";

import styles from "./ListboxButton.module.scss";
import { FlexContainer, FlexItem } from "../Flex";
import { Icon } from "../Icon";

export type ExtractProps<T> = T extends ComponentType<infer P> ? P : T;

export interface ListboxButtonProps extends ExtractProps<typeof HeadlessUIListboxButton> {
  hasError?: boolean;
}

export const ListboxButton = React.forwardRef<HTMLButtonElement, ListboxButtonProps>(
  ({ className, hasError, ...restProps }, ref) => {
    const mergedClassNames = classNames(
      styles.listboxButton,
      { [styles["listboxButton--error"]]: hasError },
      className
    );
    return (
      <HeadlessUIListboxButton {...restProps} className={mergedClassNames} ref={ref}>
        {(bag) => (
          <FlexContainer justifyContent="space-between" className={styles.listboxButton__content} alignItems="center">
            <FlexItem>
              {typeof restProps.children === "function" ? restProps.children(bag) : restProps.children}
            </FlexItem>
            <Icon type="chevronDown" color="action" />
          </FlexContainer>
        )}
      </HeadlessUIListboxButton>
    );
  }
);

ListboxButton.displayName = "ListboxButton";
