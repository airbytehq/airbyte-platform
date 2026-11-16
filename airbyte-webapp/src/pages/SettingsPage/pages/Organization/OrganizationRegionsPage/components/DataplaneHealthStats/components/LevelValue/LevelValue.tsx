import React from "react";

import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { StatusLevel, StatusLevelIcon } from "../StatusCard";

interface LevelValueProps {
  level: StatusLevel;
}

// A value with the icon of its status level in front of it.
export const LevelValue: React.FC<React.PropsWithChildren<LevelValueProps>> = ({ level, children }) => (
  <FlexContainer alignItems="center" gap="sm">
    <StatusLevelIcon level={level} />
    <Text size="sm" bold>
      {children}
    </Text>
  </FlexContainer>
);
