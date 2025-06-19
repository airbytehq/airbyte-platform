import classNames from "classnames";
import React, { HTMLAttributes } from "react";

import styles from "./FlexItem.module.scss";

interface FlexItemProps {
  className?: string;
  /**
   * Sets `flex-grow` to 1 if truthy
   */
  grow?: boolean;
  noShrink?: boolean;
  /**
   * The `align-self` css property
   */
  alignSelf?: "stretch" | "center" | "flex-start" | "flex-end";
}

/**
 * Renders a div element which sets css properties for flex children as given by the props.
 * This component can be used within a `FlexContainer` parent if grow or self-align props should be set, but it can also be omitted
 * in case no special flex properties are required.
 */
export const FlexItem: React.FC<React.PropsWithChildren<FlexItemProps & HTMLAttributes<HTMLDivElement>>> = ({
  className,
  grow,
  noShrink,
  alignSelf,
  children,
  ...otherProps
}) => {
  const fullClassName = classNames(
    {
      [styles.grow]: grow,
      [styles.noShrink]: noShrink,
      [styles.alignSelfStart]: alignSelf === "flex-start",
      [styles.alignSelfEnd]: alignSelf === "flex-end",
      [styles.alignSelfCenter]: alignSelf === "center",
      [styles.alignSelfStretch]: alignSelf === "stretch",
    },
    className
  );

  return (
    <div className={fullClassName} {...otherProps}>
      {children}
    </div>
  );
};
