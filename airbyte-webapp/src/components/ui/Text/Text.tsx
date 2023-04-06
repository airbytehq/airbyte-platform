import classNames from "classnames";
import React, { HTMLAttributes } from "react";

import styles from "./Text.module.scss";

type TextSize = "xs" | "sm" | "md" | "lg" | "xl";
type TextColor = "darkBlue" | "grey" | "grey300";
type TextElementType = "p" | "span" | "div";
type TextHTMLElement = HTMLParagraphElement | HTMLSpanElement | HTMLDivElement;

type TextProps = HTMLAttributes<TextHTMLElement> & {
  className?: string;
  centered?: boolean;
  as?: TextElementType;
  size?: TextSize;
  color?: TextColor;
  bold?: boolean;
  inverseColor?: boolean;
  title?: string;
  gradient?: boolean;
};

const sizes: Record<TextSize, string> = {
  xs: styles.xs,
  sm: styles.sm,
  md: styles.md,
  lg: styles.lg,
  xl: styles.xl,
};

const colors: Record<TextColor, string> = {
  darkBlue: styles.darkBlue,
  grey: styles.grey,
  grey300: styles.grey300,
};

const getTextClassNames = ({
  size,
  color,
  centered,
  bold,
  inverseColor,
  gradient,
}: Required<Pick<TextProps, "size" | "color" | "centered" | "bold" | "inverseColor" | "gradient">>) =>
  classNames(styles.text, sizes[size], colors[color], {
    [styles.centered]: centered,
    [styles.bold]: bold,
    [styles.inverse]: inverseColor,
    [styles.gradient]: gradient,
  });

export const Text = React.memo(
  React.forwardRef<TextHTMLElement, React.PropsWithChildren<TextProps>>(
    (
      {
        as = "p",
        bold = false,
        centered = false,
        children,
        className: classNameProp,
        size = "md",
        color = "darkBlue",
        inverseColor = false,
        gradient = false,
        ...remainingProps
      },
      ref
    ) => {
      const className = classNames(
        getTextClassNames({ centered, size, color, bold, inverseColor, gradient }),
        classNameProp
      );

      return React.createElement(
        as,
        {
          ...remainingProps,
          "data-type": "text",
          className,
          ref: ref as React.Ref<TextHTMLElement>,
        },
        children
      );
    }
  )
);
