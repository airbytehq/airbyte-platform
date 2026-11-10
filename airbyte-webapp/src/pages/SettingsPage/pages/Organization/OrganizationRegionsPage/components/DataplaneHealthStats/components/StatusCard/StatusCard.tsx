import React from "react";
import { FormattedMessage } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Icon, IconColor, IconType } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import styles from "./StatusCard.module.scss";

export type StatusLevel = "success" | "warning" | "error" | "neutral";

interface LevelIcon {
  type: IconType;
  color: IconColor;
}

const LEVEL_ICON: Record<StatusLevel, LevelIcon> = {
  success: { type: "statusSuccess", color: "success" },
  warning: { type: "statusWarning", color: "warning" },
  error: { type: "statusError", color: "error" },
  neutral: { type: "statusError", color: "disabled" },
};

/**
 * The value shown next to the level icon: either a translated message (`messageId`, optionally with `values`)
 * or plain `text` that needs no translation, such as a version number. The two cannot be mixed.
 */
type StatusValue =
  | { messageId: string; values?: Record<string, string | number>; text?: never }
  | { text: string; messageId?: never; values?: never };

export interface StatusCardProps {
  /** Title of the card, shown above the value. */
  label: React.ReactNode;
  /** Picks the icon and its color next to the value. */
  level: StatusLevel;
  /** The value shown next to the icon. */
  value: StatusValue;
  /** Optional secondary line under the value, such as "Checked 12 minutes ago". */
  caption?: React.ReactNode;
}

/**
 * A small tile that shows one status: a label, a level icon with its value, and an optional caption.
 * The tile has no width of its own, so the parent decides how wide it is.
 */
export const StatusCard: React.FC<StatusCardProps> = ({ label, level, value, caption }) => {
  const icon = LEVEL_ICON[level];

  return (
    <div className={styles.card}>
      <Text size="sm" color="grey400">
        {label}
      </Text>
      <FlexContainer alignItems="flex-end" gap="sm">
        <Icon type={icon.type} color={icon.color} size="sm" />
        <Text size="sm" bold>
          {value.text !== undefined ? value.text : <FormattedMessage id={value.messageId} values={value.values} />}
        </Text>
      </FlexContainer>
      {caption && (
        <Text size="sm" color="grey400">
          {caption}
        </Text>
      )}
    </div>
  );
};
