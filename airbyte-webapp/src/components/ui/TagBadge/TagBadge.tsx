import classNames from "classnames";

import { Icon, IconType } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import { getTextColorForBackground, isSafeHexValue } from "core/utils/color";

import styles from "./TagBadge.module.scss";

export interface TagProps {
  color?: string;
  text: string;
  icon?: IconType;
  className?: string;
}

export const TagBadge: React.FC<TagProps> = ({ color, text, icon, className }) => {
  const hasValidBackgroundColor = color && isSafeHexValue(color);
  const textColorClass =
    hasValidBackgroundColor && getTextColorForBackground(color) === "light"
      ? styles["tagBadge__text--light"]
      : styles["tagBadge__text--dark"];

  return (
    <span
      className={classNames(styles.tagBadge, className, {
        [styles["tagBadge--fallback"]]: !hasValidBackgroundColor && !className,
      })}
      style={{
        backgroundColor: hasValidBackgroundColor ? `#${color}` : undefined,
      }}
    >
      {icon && <Icon type={icon} size="xs" className={textColorClass} />}
      <Text size="sm" className={textColorClass}>
        {text}
      </Text>
    </span>
  );
};
