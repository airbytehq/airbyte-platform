import { ListboxOptions as HeadlessUIListboxOptions } from "@headlessui/react";
import classNames from "classnames";
import React, { ComponentType } from "react";

import styles from "./ListboxOptions.module.scss";

export type ExtractProps<T> = T extends ComponentType<infer P> ? P : T;

export interface OurProps {
  fullWidth?: boolean;
}

export const ListboxOptions = React.forwardRef<HTMLElement, ExtractProps<typeof HeadlessUIListboxOptions> & OurProps>(
  (props, ref) => {
    const mergedClassNames = classNames(
      styles.listboxOptions,
      {
        [styles["listboxOptions--fullWidth"]]: !!props.fullWidth,
      },
      props.className
    );
    return (
      <HeadlessUIListboxOptions {...props} className={mergedClassNames} ref={ref}>
        {props.children}
      </HeadlessUIListboxOptions>
    );
  }
);

ListboxOptions.displayName = "ListboxOptions";
