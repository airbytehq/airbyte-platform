import { FormattedMessage } from "react-intl";

import { EmptyState } from "components/EmptyState";
import { FlexContainer } from "components/ui/Flex";

import { AddStreamForMappingComboBox } from "./AddStreamForMappingComboBox";

export const MappingsEmptyState: React.FC = () => {
  return (
    <FlexContainer direction="column" alignItems="center" gap="xl">
      <EmptyState
        icon="mapping"
        description={
          <>
            <FormattedMessage id="connections.mappings.description" />
            <br />
            <br />
            <FormattedMessage id="connections.mappings.selectStream" />
          </>
        }
      />
      <FlexContainer direction="column" gap="none" alignItems="center">
        <AddStreamForMappingComboBox />
      </FlexContainer>
    </FlexContainer>
  );
};
