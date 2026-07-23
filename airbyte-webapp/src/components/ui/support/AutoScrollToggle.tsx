import React from "react";
import { FormattedMessage } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Switch } from "components/ui/Switch";
import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import styles from "./AutoScrollToggle.module.scss";

interface AutoScrollToggleProps {
  checked: boolean;
  onChange: (checked: boolean) => void;
}

export const AutoScrollToggle: React.FC<AutoScrollToggleProps> = ({ checked, onChange }) => {
  return (
    <Tooltip
      placement="top"
      control={
        <FlexContainer alignItems="center" gap="xs" className={styles.toggleContainer}>
          <Text size="xs" color="grey">
            <FormattedMessage id="chat.autoScroll.label" />
          </Text>
          <Switch
            checked={checked}
            onChange={(e) => onChange(e.target.checked)}
            size="xs"
            aria-label="Toggle auto-scroll"
          />
        </FlexContainer>
      }
    >
      <FormattedMessage id={checked ? "chat.autoScroll.tooltip.enabled" : "chat.autoScroll.tooltip.disabled"} />
    </Tooltip>
  );
};
