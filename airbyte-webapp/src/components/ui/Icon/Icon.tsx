import classNames from "classnames";
import React from "react";

import styles from "./Icon.module.scss";
import { ReactComponent as ArrowRightIcon } from "./icons/arrowRightIcon.svg";
import { ReactComponent as ChevronLeftIcon } from "./icons/chevronLeftIcon.svg";
import { ReactComponent as ChevronRightIcon } from "./icons/chevronRightIcon.svg";
import { ReactComponent as CreditsIcon } from "./icons/creditsIcon.svg";
import { ReactComponent as CrossIcon } from "./icons/crossIcon.svg";
import { ReactComponent as DocsIcon } from "./icons/docsIcon.svg";
import { ReactComponent as GAIcon } from "./icons/gAIcon.svg";
import { ReactComponent as InfoIcon } from "./icons/infoIcon.svg";
import { ReactComponent as LensIcon } from "./icons/lensIcon.svg";
import { ReactComponent as MinusIcon } from "./icons/minusIcon.svg";
import { ReactComponent as ModificationIcon } from "./icons/modificationIcon.svg";
import { ReactComponent as MoonIcon } from "./icons/moonIcon.svg";
import { ReactComponent as NestedIcon } from "./icons/nestedIcon.svg";
import { ReactComponent as PauseIcon } from "./icons/pauseIcon.svg";
import { ReactComponent as PencilIcon } from "./icons/pencilIcon.svg";
import { ReactComponent as PlayIcon } from "./icons/playIcon.svg";
import { ReactComponent as PlusIcon } from "./icons/plusIcon.svg";
import { ReactComponent as RotateIcon } from "./icons/rotateIcon.svg";
import { IconColor, IconProps, IconType } from "./types";

const colorMap: Record<IconColor, string> = {
  warning: styles[`icon--warning`],
  success: styles[`icon--success`],
  primary: styles[`icon--primary`],
  disabled: styles[`icon--disabled`],
  error: styles[`icon--error`],
  action: styles[`icon--action`],
  affordance: styles[`icon--affordance`],
};

const sizeMap: Record<NonNullable<IconProps["size"]>, string> = {
  xs: styles.xs,
  sm: styles.sm,
  md: styles.md,
  lg: styles.lg,
  xl: styles.xl,
};

const Icons: Record<IconType, React.FC<React.SVGProps<SVGSVGElement>>> = {
  arrowRight: ArrowRightIcon,
  credits: CreditsIcon,
  cross: CrossIcon,
  docs: DocsIcon,
  ga: GAIcon,
  info: InfoIcon,
  minus: MinusIcon,
  modification: ModificationIcon,
  moon: MoonIcon,
  pause: PauseIcon,
  pencil: PencilIcon,
  play: PlayIcon,
  plus: PlusIcon,
  rotate: RotateIcon,
  nested: NestedIcon,
  chevronLeft: ChevronLeftIcon,
  chevronRight: ChevronRightIcon,
  lens: LensIcon,
};

export const Icon: React.FC<IconProps> = React.memo(
  ({ type, color, size = "md", withBackground, className, ...props }) => {
    const classes = classNames(
      className,
      styles.icon,
      color ? colorMap[color] : undefined,
      withBackground ? styles["icon--withBackground"] : undefined,
      sizeMap[size]
    );

    return React.createElement(Icons[type], {
      ...props,
      className: classes,
    });
  }
);
Icon.displayName = "Icon";
