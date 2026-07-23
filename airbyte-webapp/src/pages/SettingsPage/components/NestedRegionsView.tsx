import { FormattedMessage } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";

import { NestedRegionCard } from "./NestedRegionCard";
import { useNestedRegionsData } from "./useNestedRegionsData";

export const NestedRegionsView: React.FC = () => {
  const { regionsData } = useNestedRegionsData();

  return (
    <FlexContainer direction="column" gap="lg">
      <Heading as="h2" size="sm">
        <FormattedMessage id="settings.organizationSettings.regions" />
      </Heading>

      <FlexContainer direction="column" gap="md">
        {regionsData.map((region) => (
          <NestedRegionCard
            key={region.region_id}
            regionName={region.region_name}
            dataplanes={region.dataplanes}
            workspaces={region.workspaces}
          />
        ))}
      </FlexContainer>
    </FlexContainer>
  );
};
