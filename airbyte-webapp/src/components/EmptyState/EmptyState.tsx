import React from "react";

import { FlexContainer } from "components/ui/Flex";
import { Icon, IconProps } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import styles from "./EmptyState.module.scss";

interface EmptyStateProps {
  icon?: IconProps["type"];
  text: React.ReactNode;
  description?: React.ReactNode;
  button?: React.ReactNode;
}

export const EmptyState: React.FC<EmptyStateProps> = ({ icon = "cactus", text, description, button }) => (
  <FlexContainer alignItems="center" direction="column">
    <FlexContainer alignItems="center" justifyContent="center" className={styles.circle}>
      <Icon type={icon} color="action" />
    </FlexContainer>
    <Text color="grey500" size="lg">
      {text}
    </Text>
    {description && <Text color="grey400">{description}</Text>}
    {button}
  </FlexContainer>
);
