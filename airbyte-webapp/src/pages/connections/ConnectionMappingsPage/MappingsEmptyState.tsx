import { FormattedMessage } from "react-intl";

import { EmptyState } from "components/EmptyState";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";

import { AddStreamForMappingComboBox } from "./AddStreamForMappingComboBox";

export const MappingsEmptyState: React.FC = () => {
  return (
    <FlexContainer direction="column" gap="lg">
      <Heading as="h3" size="sm">
        <FormattedMessage id="connection.mappings.title" />
      </Heading>

      <FlexContainer direction="column" alignItems="center" gap="xl">
        <EmptyState icon="mapping" text={<FormattedMessage id="connection.mappings.emptyState" />} />
        <FlexContainer direction="column" gap="none" alignItems="center">
          <AddStreamForMappingComboBox
            onStreamSelected={(streamName: string) => {
              // this is intentionally here! i will be adding the editor view + form state, etc. in a subsequent PR
              console.log(streamName);
            }}
          />
        </FlexContainer>
      </FlexContainer>
    </FlexContainer>
  );
};
