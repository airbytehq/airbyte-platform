import classNames from "classnames";
import React from "react";

import styles from "./FlexContainer.module.scss";

interface FlexContainerProps {
  as?: "div" | "span";
  className?: string;
  display?: "flex" | "inline-flex";
  /**
   * The flex-direction css property
   */
  direction?: "row" | "column" | "row-reverse" | "column-reverse";
  /**
   * gap between the flex items - defaults to `md` if not provided. None means no gap is applied, the others map to the respective scss spacing variable.
   */
  gap?: "none" | "xs" | "sm" | "md" | "lg" | "xl" | "2xl";
  /**
   * The align-items css property
   */
  alignItems?: "flex-start" | "flex-end" | "center" | "baseline" | "stretch";
  /**
   * The justify-content css property
   */
  justifyContent?: "flex-start" | "flex-end" | "center" | "space-between" | "space-around" | "space-evenly";
  /**
   * The flex-wrap css property
   */
  wrap?: "wrap" | "nowrap" | "wrap-reverse";
}

/**
 * Renders a div element which layouts its children as flex items as specified by the props.
 *  Children of a `FlexContainer` can but don't have to be `FlexItem` elements.
 */
export const FlexContainer = React.forwardRef<
  HTMLDivElement,
  FlexContainerProps & React.ComponentPropsWithoutRef<"div">
>(
  (
    {
      as = "div",
      className,
      direction = "row",
      display = "flex",
      gap = "md",
      alignItems = "stretch",
      justifyContent = "flex-start",
      wrap = "nowrap",
      children,
      ...otherProps
    },
    ref
  ) => {
    const fullClassName = classNames(
      {
        [styles["container--inline"]]: display === "inline-flex",
        [styles.directionRow]: direction === "row",
        [styles.directionColumn]: direction === "column",
        [styles.directionRowReverse]: direction === "row-reverse",
        [styles.directionColumnReverse]: direction === "column-reverse",
        [styles.gapXs]: gap === "xs",
        [styles.gapSm]: gap === "sm",
        [styles.gapMd]: gap === "md",
        [styles.gapLg]: gap === "lg",
        [styles.gapXl]: gap === "xl",
        [styles.gap2xl]: gap === "2xl",
        [styles.alignItemsStart]: alignItems === "flex-start",
        [styles.alignItemsEnd]: alignItems === "flex-end",
        [styles.alignItemsCenter]: alignItems === "center",
        [styles.alignItemsBaseline]: alignItems === "baseline",
        [styles.alignItemsStretch]: alignItems === "stretch",
        [styles.justifyContentStart]: justifyContent === "flex-start",
        [styles.justifyContentEnd]: justifyContent === "flex-end",
        [styles.justifyContentCenter]: justifyContent === "center",
        [styles.justifyContentBetween]: justifyContent === "space-between",
        [styles.justifyContentAround]: justifyContent === "space-around",
        [styles.justifyContentEvenly]: justifyContent === "space-evenly",
        [styles.wrapWrap]: wrap === "wrap",
        [styles.wrapNowrap]: wrap === "nowrap",
        [styles.wrapWrapReverse]: wrap === "wrap-reverse",
      },
      styles.container,
      className
    );

    return React.createElement(as, {
      ...otherProps,
      ref,
      className: fullClassName,
      children,
    });
  }
);
FlexContainer.displayName = "FlexContainer";
