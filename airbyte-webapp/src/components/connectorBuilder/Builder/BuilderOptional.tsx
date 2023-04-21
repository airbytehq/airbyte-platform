import React from "react";
import { useIntl } from "react-intl";

import { Collapsible } from "components/ui/Collapsible";
import { FlexContainer } from "components/ui/Flex";

interface BuilderOptionalProps {
  label?: string;
}

export const BuilderOptional: React.FC<React.PropsWithChildren<BuilderOptionalProps>> = ({ children, label }) => {
  const { formatMessage } = useIntl();

  return (
    <Collapsible label={label ?? formatMessage({ id: "connectorBuilder.optionalFieldsLabel" })} type="footer">
      <FlexContainer direction="column" gap="xl">
        {children}
      </FlexContainer>
    </Collapsible>
  );
};
