import React, { PropsWithChildren } from "react";

import { FlexContainer } from "../Flex";

interface TabsProps {
  className?: string;
}

export const Tabs: React.FC<PropsWithChildren<TabsProps>> = ({ children, className }) => {
  return (
    <FlexContainer className={className} alignItems="baseline" justifyContent="flex-start" gap="lg">
      {children}
    </FlexContainer>
  );
};
