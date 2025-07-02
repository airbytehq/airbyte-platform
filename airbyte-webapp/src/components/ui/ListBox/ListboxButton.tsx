import { ListboxButton as HeadlessUIListboxButton } from "@headlessui/react";
import classNames from "classnames";
import React, { ComponentType } from "react";

import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";

import styles from "./ListboxButton.module.scss";

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
          <FlexContainer
            justifyContent="space-between"
            className={styles.listboxButton__content}
            alignItems="center"
            as="span"
          >
            <FlexItem className={styles.listboxButton__children}>
              {typeof restProps.children === "function" ? restProps.children(bag) : restProps.children}
            </FlexItem>
            <Icon type="chevronDown" color="action" className={styles.listboxButton__caret} />
          </FlexContainer>
        )}
      </HeadlessUIListboxButton>
    );
  }
);

ListboxButton.displayName = "ListboxButton";
