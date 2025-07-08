import classNames from "classnames";
import React, { HTMLAttributes } from "react";

import { HeadingElementType } from "components/ui/Heading";

import styles from "./Text.module.scss";

export type TextSize = "xs" | "sm" | "md" | "lg" | "xl";
export type TextColor =
  | "darkBlue"
  | "grey"
  | "grey300"
  | "green"
  | "green600"
  | "red"
  | "red200"
  | "red400"
  | "grey600"
  | "grey400"
  | "grey500"
  | "blue"
  | "yellow600";
type TextElementType = "p" | "span" | "div" | HeadingElementType;
type TextHTMLElement = HTMLParagraphElement | HTMLSpanElement | HTMLDivElement;

type TextAlignment = "left" | "center" | "right";

export type TextProps = HTMLAttributes<TextHTMLElement> & {
  className?: string;
  as?: TextElementType;
  size?: TextSize;
  color?: TextColor;
  bold?: boolean;
  inverseColor?: boolean;
  title?: string;
  gradient?: boolean;
  align?: TextAlignment;
  italicized?: boolean;
  smallcaps?: boolean;
  "data-testid"?: string;
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
  green: styles.green,
  green600: styles.green600,
  grey: styles.grey,
  grey300: styles.grey300,
  red: styles.red,
  red200: styles.red200,
  red400: styles.red400,
  blue: styles.blue,
  grey400: styles.grey400,
  grey500: styles.grey500,
  grey600: styles.grey600,
  yellow600: styles.yellow600,
};

const textAlignments: Record<TextAlignment, string> = {
  left: styles.left,
  center: styles.center,
  right: styles.right,
};

const getTextClassNames = ({
  size,
  color,
  bold,
  inverseColor,
  gradient,
  align,
  italicized,
  smallcaps,
}: Required<
  Pick<TextProps, "size" | "color" | "bold" | "inverseColor" | "gradient" | "align" | "italicized" | "smallcaps">
>) =>
  classNames(styles.text, sizes[size], colors[color], textAlignments[align], {
    [styles.bold]: bold,
    [styles.inverse]: inverseColor,
    [styles.gradient]: gradient,
    [styles.italicized]: italicized,
    [styles.smallcaps]: smallcaps,
  });

export const Text = React.memo(
  React.forwardRef<TextHTMLElement, React.PropsWithRef<React.PropsWithChildren<TextProps>>>(
    (
      {
        as = "p",
        bold = false,
        children,
        className: classNameProp,
        size = "md",
        color = "darkBlue",
        inverseColor = false,
        gradient = false,
        align = "left",
        italicized = false,
        smallcaps = false,
        ...remainingProps
      },
      ref
    ) => {
      const className = classNames(
        getTextClassNames({ size, color, bold, inverseColor, gradient, align, italicized, smallcaps }),
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
