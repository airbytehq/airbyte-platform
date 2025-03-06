import { FormattedMessage } from "react-intl";

import { EmptyState } from "components/EmptyState";
import { FlexContainer } from "components/ui/Flex";

import { AddStreamForMappingComboBox } from "./AddStreamForMappingComboBox";

export const MappingsEmptyState: React.FC = () => {
  return (
    <FlexContainer direction="column" alignItems="center" gap="xl">
      <EmptyState icon="mapping" text={<FormattedMessage id="connections.mappings.emptyState" />} />
      <FlexContainer direction="column" gap="none" alignItems="center">
        <AddStreamForMappingComboBox />
      </FlexContainer>
    </FlexContainer>
  );
};
