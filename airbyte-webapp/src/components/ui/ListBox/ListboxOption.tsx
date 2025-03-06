import { ListboxOption as HeadlessUIListboxOption } from "@headlessui/react";
import classNames from "classnames";
import { ComponentType } from "react";

import styles from "./ListboxOption.module.scss";

export type ExtractProps<T> = T extends ComponentType<infer P> ? P : T;

export const ListboxOption = (props: ExtractProps<typeof HeadlessUIListboxOption>) => {
  const mergedClassNames = classNames(styles.listboxOption, props.className);
  return (
    <HeadlessUIListboxOption {...props} className={mergedClassNames}>
      {props.children}
    </HeadlessUIListboxOption>
  );
};
