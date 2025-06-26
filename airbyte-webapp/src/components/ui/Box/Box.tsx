import classNames from "classnames";
import React, { forwardRef, PropsWithChildren } from "react";

import styles from "./Box.module.scss";

type SpacingSize = "none" | "xs" | "sm" | "md" | "lg" | "xl" | "2xl";

interface BoxProps {
  as?: keyof HTMLElementTagNameMap;
  className?: string;
  p?: SpacingSize;
  py?: SpacingSize;
  px?: SpacingSize;
  pt?: SpacingSize;
  pr?: SpacingSize;
  pb?: SpacingSize;
  pl?: SpacingSize;
  m?: SpacingSize;
  my?: SpacingSize;
  mx?: SpacingSize;
  mt?: SpacingSize;
  mr?: SpacingSize;
  mb?: SpacingSize;
  ml?: SpacingSize;
  "data-testid"?: string;
}

function toClassName(key: keyof Omit<BoxProps, "className" | "as" | "data-testid">, value: SpacingSize | undefined) {
  if (!value) {
    return undefined;
  }
  return styles[`box--${key}-${value}`];
}

const keys = ["m", "my", "mx", "mt", "mr", "mb", "ml", "p", "py", "px", "pt", "pr", "pb", "pl"] as const;

export const Box = forwardRef<HTMLElement, PropsWithChildren<BoxProps>>(
  ({ as = "div", children, className: classNameProp, ...props }, ref) => {
    const className = classNames(classNameProp, styles.box, ...keys.map((key) => toClassName(key, props[key])));

    return React.createElement(as, { className, children, "data-testid": props["data-testid"], ref });
  }
);
Box.displayName = "Box";
