import classnames from "classnames";
import React from "react";

import styles from "./ScrollParent.module.scss";

export const ScrollParentContext = React.createContext<HTMLDivElement | null>(null);

interface ScrollParentProps<T> {
  as?: T extends React.ElementType ? T : never;
  props?: T extends React.ElementType ? React.ComponentPropsWithoutRef<T> : never;
}

export const ScrollParent = <T = "div",>({ children, as, props }: React.PropsWithChildren<ScrollParentProps<T>>) => {
  const [ref, setRef] = React.useState<HTMLDivElement | null>(null);
  const Component = as || "div";

  const { className, ...rest } = props || ({} as React.ComponentPropsWithoutRef<"div">);
  return (
    <ScrollParentContext.Provider value={ref}>
      <Component className={classnames(className, styles.container)} {...rest} ref={setRef}>
        {children}
      </Component>
    </ScrollParentContext.Provider>
  );
};
