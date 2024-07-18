import classNames from "classnames";
import { forwardRef, PropsWithChildren } from "react";

import styles from "./ScrollableContainer.module.scss";

/**
 * Layout component for ConnectionPage to define the scrollable area and pinpoint the container for Virtuoso,
 * in case we need to virtualize something on the page.
 */
export const ScrollableContainer = forwardRef<HTMLDivElement, PropsWithChildren & { className?: string }>(
  ({ children, className, ...restProps }, outerRef) => (
    <div ref={outerRef} className={classNames(styles.container, className)} {...restProps}>
      {children}
    </div>
  )
);
ScrollableContainer.displayName = "ScrollableContainer";
