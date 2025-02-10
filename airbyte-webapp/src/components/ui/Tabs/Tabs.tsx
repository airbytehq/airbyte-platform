import React, { ComponentProps, PropsWithChildren } from "react";

import { FlexContainer } from "../Flex";

interface TabsProps {
  className?: string;
  gap?: ComponentProps<typeof FlexContainer>["gap"];
}

export const Tabs: React.FC<PropsWithChildren<TabsProps>> = ({ children, className, gap = "lg" }) => {
  return (
    <FlexContainer className={className} alignItems="baseline" justifyContent="flex-start" gap={gap}>
      {children}
    </FlexContainer>
  );
};
