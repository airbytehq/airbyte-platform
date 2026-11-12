import React, { useId } from "react";

import { CheckBox } from "components/ui/CheckBox";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { ConnectorIcon } from "area/connector/components/ConnectorIcon";
import { SourceDefinitionRead } from "core/api/types/AirbyteClient";

interface SourceCheckboxProps {
  definition: SourceDefinitionRead;
  checked: boolean;
  onToggle: (sourceDefinitionId: string) => void;
}

/** A connector can show in a category and in the search results at once, so each row needs its own input id. */
export const SourceCheckbox: React.FC<SourceCheckboxProps> = ({ definition, checked, onToggle }) => {
  const inputId = useId();

  return (
    <FlexContainer alignItems="center" gap="md">
      <ConnectorIcon icon={definition.icon} />
      <CheckBox id={inputId} checked={checked} onChange={() => onToggle(definition.sourceDefinitionId)} />
      <label htmlFor={inputId}>
        <Text size="sm">{definition.name}</Text>
      </label>
    </FlexContainer>
  );
};
