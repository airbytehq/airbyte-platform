import React from "react";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import styles from "./StatusCard.module.scss";
import { StatusLevel, StatusLevelIcon } from "./StatusLevelIcon";

// The value is either a translated message or plain text, never both.
type StatusValue =
  | { messageId: string; values?: Record<string, string | number>; text?: never }
  | { text: string; messageId?: never; values?: never };

export interface StatusCardProps {
  label: React.ReactNode;
  level: StatusLevel;
  value: StatusValue;
  caption?: React.ReactNode;
  // Leave out for a card with no details to show.
  details?: {
    isExpanded: boolean;
    onToggle: () => void;
    // The id of the element that shows the details, so assistive technology can link the two.
    panelId: string;
  };
}

// The tile has no width of its own, so the parent decides how wide it is.
export const StatusCard: React.FC<StatusCardProps> = ({ label, level, value, caption, details }) => {
  return (
    <div className={styles.card}>
      <FlexContainer alignItems="baseline" justifyContent="space-between" gap="sm">
        <Text size="sm" color="grey400">
          {label}
        </Text>
        {details && (
          <Button
            variant="link"
            size="xs"
            type="button"
            className={styles.detailsToggle}
            aria-expanded={details.isExpanded}
            aria-controls={details.panelId}
            onClick={details.onToggle}
          >
            <FormattedMessage
              id={
                details.isExpanded
                  ? "settings.organization.regions.dataplane.hideDetails"
                  : "settings.organization.regions.dataplane.viewDetails"
              }
            />
          </Button>
        )}
      </FlexContainer>
      <FlexContainer alignItems="flex-end" gap="sm">
        <StatusLevelIcon level={level} />
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
