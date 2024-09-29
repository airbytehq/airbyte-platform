import { PropsWithChildren } from "react";

import { FlexContainer } from "../Flex";
import { Icon } from "../Icon";
import { Text } from "../Text";

export const DataLoadingError: React.FC<PropsWithChildren> = ({ children }) => {
  return (
    <FlexContainer alignItems="center">
      <Icon type="errorOutline" color="error" />
      <Text>{children}</Text>
    </FlexContainer>
  );
};
