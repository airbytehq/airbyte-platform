import { ListboxButton as HeadlessUIListboxButton } from "@headlessui/react";
import classNames from "classnames";
import React, { ComponentType } from "react";

import styles from "./ListboxButton.module.scss";
import { FlexContainer, FlexItem } from "../Flex";
import { Icon } from "../Icon";

export type ExtractProps<T> = T extends ComponentType<infer P> ? P : T;

export const ListboxButton = React.forwardRef<HTMLButtonElement, ExtractProps<typeof HeadlessUIListboxButton>>(
  (props, ref) => {
    const mergedClassNames = classNames(styles.listboxButton, props.className);
    return (
      <HeadlessUIListboxButton {...props} className={mergedClassNames} ref={ref}>
        {(bag) => (
          <FlexContainer justifyContent="space-between" className={styles.listboxButton__content} alignItems="center">
            <FlexItem>{typeof props.children === "function" ? props.children(bag) : props.children}</FlexItem>
            <Icon type="chevronDown" color="action" />
          </FlexContainer>
        )}
      </HeadlessUIListboxButton>
    );
  }
);

ListboxButton.displayName = "ListboxButton";
