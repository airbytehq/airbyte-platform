import React, { PropsWithChildren } from "react";

import { FlexContainer } from "../Flex";

export const Tabs: React.FC<PropsWithChildren<unknown>> = ({ children }) => {
  return (
    <FlexContainer alignItems="baseline" justifyContent="flex-start" gap="lg">
      {children}
    </FlexContainer>
  );
};
