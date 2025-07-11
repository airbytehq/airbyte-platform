import React, { useMemo } from "react";
import { FormattedMessage } from "react-intl";

import { ConnectorIcon } from "components/ConnectorIcon";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Text, TextColor } from "components/ui/Text";

import { useDestinationDefinitionList, useSourceDefinitionList } from "core/api";
import { naturalComparatorBy } from "core/utils/objects";

import { SummaryKey } from "../ConnectionsSummary";

type filterIconType = "successFilled" | "errorFilled" | "sync" | "pauseFilled";

const generateStatusFilterOption = (value: SummaryKey, id: string, iconType: filterIconType, color: TextColor) => ({
  label: (
    <FlexContainer gap="md" alignItems="center">
      <FlexItem>
        <Text color={color} as="span">
          <Icon type={iconType} size="md" />
        </Text>
      </FlexItem>
      <FlexItem>
        <Text color="grey" bold as="span">
          <FormattedMessage id={id} />
        </Text>
      </FlexItem>
    </FlexContainer>
  ),
  value,
});

const generateStateFilterOption = (value: "active" | "inactive" | null, id: string) => ({
  label: (
    <Text color="grey" bold as="span">
      <FormattedMessage id={id} />
    </Text>
  ),
  value,
});

export const statusFilterOptions = [
  {
    label: (
      <Text color="grey" bold>
        <FormattedMessage id="tables.connections.filters.status.all" />
      </Text>
    ),
    value: null,
  },
  generateStatusFilterOption("healthy", "tables.connections.filters.status.healthy", "successFilled", "green600"),
  generateStatusFilterOption("failed", "tables.connections.filters.status.failed", "errorFilled", "red"),
  generateStatusFilterOption("running", "tables.connections.filters.status.running", "sync", "blue"),
];

export const stateFilterOptions = [
  {
    label: (
      <Text color="grey" bold>
        <FormattedMessage id="tables.connections.filters.state.all" />
      </Text>
    ),
    value: null,
  },
  generateStateFilterOption("active", "tables.connections.filters.state.enabled"),
  generateStateFilterOption("inactive", "tables.connections.filters.state.disabled"),
];

interface FilterOption {
  label: React.ReactNode;
  value: string | null;
}

type SortableFilterOption = FilterOption & { sortValue: string };

export const useAvailableSourceOptions = (): SortableFilterOption[] => {
  const { sourceDefinitions } = useSourceDefinitionList({ filterByUsed: true });

  return useMemo(
    () =>
      [
        {
          label: (
            <Text bold color="grey">
              <FormattedMessage id="tables.connections.filters.source.all" />
            </Text>
          ),
          value: null,
          sortValue: "",
        },
        ...sourceDefinitions.map((definition) => {
          return {
            label: (
              <FlexContainer gap="sm" alignItems="center" as="span">
                <FlexItem>
                  <ConnectorIcon icon={definition.icon} />
                </FlexItem>
                <FlexItem>
                  <Text size="sm">{definition.name}</Text>
                </FlexItem>
              </FlexContainer>
            ),
            value: definition.sourceDefinitionId,
            sortValue: definition.name,
          };
        }),
      ].sort(naturalComparatorBy((option) => option.sortValue)),
    [sourceDefinitions]
  );
};

export const useAvailableDestinationOptions = (): SortableFilterOption[] => {
  const { destinationDefinitions } = useDestinationDefinitionList({ filterByUsed: true });

  return useMemo(
    () =>
      [
        {
          label: (
            <Text bold color="grey">
              <FormattedMessage id="tables.connections.filters.destination.all" />
            </Text>
          ),
          value: null,
          sortValue: "",
        },
        ...destinationDefinitions.map((definition) => {
          return {
            label: (
              <FlexContainer gap="sm" alignItems="center" as="span">
                <FlexItem>
                  <ConnectorIcon icon={definition.icon} />
                </FlexItem>
                <FlexItem>
                  <Text size="sm">{definition.name}</Text>
                </FlexItem>
              </FlexContainer>
            ),
            value: definition.destinationDefinitionId,
            sortValue: definition.name,
          };
        }),
      ].sort(naturalComparatorBy((option) => option.sortValue)),
    [destinationDefinitions]
  );
};
