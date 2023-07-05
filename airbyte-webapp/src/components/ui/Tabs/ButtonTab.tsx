import classNames from "classnames";
import React, { useCallback } from "react";

import styles from "./ButtonTab.module.scss";
import { Text } from "../Text";

interface ButtonTabProps {
  id: string;
  name: string | React.ReactNode;
  isActive: boolean;
  disabled?: boolean;
  onSelect: (id: string) => void;
}

export const ButtonTab: React.FC<ButtonTabProps> = ({ name, id, isActive, onSelect, disabled }) => {
  const onItemClickItem = useCallback(() => onSelect?.(id), [id, onSelect]);

  return (
    <button
      disabled={!onSelect || disabled}
      onClick={onItemClickItem}
      className={classNames(styles.tabContainer, {
        [styles["tabContainer--active"]]: isActive,
        [styles["tabContainer--inactive"]]: !isActive,
      })}
      data-id={`${id.toLowerCase()}-step`}
    >
      <Text color={isActive ? "darkBlue" : "grey"} className={styles.text} size="lg">
        {name}
      </Text>
    </button>
  );
};
