import { ListboxOptions as HeadlessUIListboxOptions } from "@headlessui/react";
import classNames from "classnames";
import React, { ComponentType } from "react";

import styles from "./ListboxOptions.module.scss";

export type ExtractProps<T> = T extends ComponentType<infer P> ? P : T;

export interface ListboxOptionsProps {
  fullWidth?: boolean;
}

export const ListboxOptions = React.forwardRef<
  HTMLElement,
  ExtractProps<typeof HeadlessUIListboxOptions> & ListboxOptionsProps
>((props, ref) => {
  const { fullWidth, ...restProps } = props;
  const mergedClassNames = classNames(
    styles.listboxOptions,
    {
      [styles["listboxOptions--fullWidth"]]: !!fullWidth,
    },
    props.className
  );
  return (
    <HeadlessUIListboxOptions {...restProps} className={mergedClassNames} ref={ref}>
      {props.children}
    </HeadlessUIListboxOptions>
  );
});

ListboxOptions.displayName = "ListboxOptions";
