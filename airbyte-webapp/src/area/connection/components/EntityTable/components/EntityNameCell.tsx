import classNames from "classnames";
import React from "react";

import { TextWithOverflowTooltip } from "components/ui/Text";

import styles from "./EntityNameCell.module.scss";

interface EntityNameCellProps {
  value: string;
  enabled: boolean;
  className?: string;
}

export const EntityNameCell: React.FC<EntityNameCellProps> = ({ value, enabled, className }) => {
  return (
    <TextWithOverflowTooltip className={classNames(styles.text, { [styles.enabled]: enabled }, className)} size="sm">
      {value}
    </TextWithOverflowTooltip>
  );
};
