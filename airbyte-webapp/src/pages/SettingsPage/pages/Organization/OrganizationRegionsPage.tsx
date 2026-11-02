import React, { useMemo, useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { ListBox, Option } from "components/ui/ListBox";
import { SearchInput } from "components/ui/SearchInput";
import { Text } from "components/ui/Text";

import { PageTrackingCodes, useTrackPage } from "core/services/analytics";

import { RegionCard } from "./components/Regions/RegionCard";
import { filterRegions, HealthFilter, RegionTypeFilter } from "./components/Regions/regionsPageData";
import { useRegionsPageData } from "./components/Regions/useRegionsPageData";

export const OrganizationRegionsPage: React.FC = () => {
  useTrackPage(PageTrackingCodes.SETTINGS_ORGANIZATION_REGIONS);
  const { formatMessage } = useIntl();
  const { regions, workspacesStatus } = useRegionsPageData();

  const [search, setSearch] = useState("");
  const [regionType, setRegionType] = useState<RegionTypeFilter>("all");
  const [health, setHealth] = useState<HealthFilter>("all");

  const regionTypeOptions: Array<Option<RegionTypeFilter>> = [
    { label: formatMessage({ id: "settings.organization.regions.filter.regionType.all" }), value: "all" },
    {
      label: formatMessage({ id: "settings.organization.regions.filter.regionType.airbyteManaged" }),
      value: "airbyteManaged",
    },
    {
      label: formatMessage({ id: "settings.organization.regions.filter.regionType.selfManaged" }),
      value: "selfManaged",
    },
  ];

  const healthOptions: Array<Option<HealthFilter>> = [
    { label: formatMessage({ id: "settings.organization.regions.filter.health.all" }), value: "all" },
    { label: formatMessage({ id: "dataplaneHealth.status.healthy" }), value: "HEALTHY" },
    { label: formatMessage({ id: "dataplaneHealth.status.degraded" }), value: "DEGRADED" },
    { label: formatMessage({ id: "dataplaneHealth.status.unhealthy" }), value: "UNHEALTHY" },
    { label: formatMessage({ id: "settings.organization.regions.neverConnected" }), value: "UNKNOWN" },
  ];

  const visibleRegions = useMemo(
    () => filterRegions(regions, { search, regionType, health }),
    [regions, search, regionType, health]
  );

  return (
    <FlexContainer direction="column" gap="lg">
      <Heading as="h1" size="md">
        <FormattedMessage id="settings.organizationSettings.regions" />
      </Heading>

      <FlexContainer direction="column" gap="md">
        <SearchInput
          value={search}
          onChange={setSearch}
          placeholder={formatMessage({ id: "settings.organization.regions.search.placeholder" })}
          data-testid="regions-search"
        />
        <FlexContainer gap="md">
          <FlexItem>
            <ListBox
              options={regionTypeOptions}
              selectedValue={regionType}
              onSelect={setRegionType}
              data-testid="regions-type-filter"
            />
          </FlexItem>
          <FlexItem>
            <ListBox
              options={healthOptions}
              selectedValue={health}
              onSelect={setHealth}
              data-testid="regions-health-filter"
            />
          </FlexItem>
        </FlexContainer>
      </FlexContainer>

      {visibleRegions.length === 0 ? (
        <Text color="grey" size="md" data-testid="regions-empty">
          <FormattedMessage id="settings.organization.regions.noResults" />
        </Text>
      ) : (
        <FlexContainer direction="column" gap="md">
          {visibleRegions.map((region) => (
            <RegionCard key={region.regionId} region={region} workspacesStatus={workspacesStatus} />
          ))}
        </FlexContainer>
      )}
    </FlexContainer>
  );
};
