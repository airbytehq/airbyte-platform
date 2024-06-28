import React from "react";

import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import cactus from "./cactus.svg";

interface EmptyResourceBlockProps {
  text: React.ReactNode;
  description?: React.ReactNode;
}

export const EmptyResourceBlock: React.FC<EmptyResourceBlockProps> = ({ text, description }) => (
  <FlexContainer alignItems="center" direction="column">
    <img src={cactus} height={46} alt="" />
    <FlexContainer alignItems="center" direction="column" gap="sm">
      <Text color="grey500" size="lg">
        {text}
      </Text>
      {description && <Text color="grey400">{description}</Text>}
    </FlexContainer>
  </FlexContainer>
);
