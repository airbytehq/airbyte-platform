import React from "react";

import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";

interface OnboardingScreenLayoutProps {
  title: React.ReactNode;
  actions: React.ReactNode;
}

export const OnboardingScreenLayout: React.FC<React.PropsWithChildren<OnboardingScreenLayoutProps>> = ({
  title,
  actions,
  children,
}) => (
  <FlexContainer direction="column" gap="xl">
    <Heading as="h1" size="lg">
      {title}
    </Heading>
    {children}
    {actions}
  </FlexContainer>
);
