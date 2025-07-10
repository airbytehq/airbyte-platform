import React, { useMemo } from "react";
import { FormattedMessage } from "react-intl";

import { ConnectorIcon } from "components/ConnectorIcon";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Text, TextColor } from "components/ui/Text";

import { useDestinationList, useSourceList } from "core/api";
import { DestinationRead, SourceRead } from "core/api/types/AirbyteClient";
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
  const { sources } = useSourceList();

  return useMemo(() => {
    const dedupedSourceDefinitions = new Map<string, SourceRead>();
    sources.forEach((source) => {
      if (!dedupedSourceDefinitions.has(source.sourceDefinitionId)) {
        dedupedSourceDefinitions.set(source.sourceDefinitionId, source);
      }
    });
    return [
      {
        label: (
          <Text bold color="grey">
            <FormattedMessage id="tables.connections.filters.source.all" />
          </Text>
        ),
        value: null,
        sortValue: "",
      },
      ...Array.from(dedupedSourceDefinitions).map(([, source]) => {
        return {
          label: (
            <FlexContainer gap="sm" alignItems="center" as="span">
              <FlexItem>
                <ConnectorIcon icon={source.icon} />
              </FlexItem>
              <FlexItem>
                <Text size="sm">{source.sourceName}</Text>
              </FlexItem>
            </FlexContainer>
          ),
          value: source.sourceDefinitionId,
          sortValue: source.sourceName,
        };
      }),
    ].sort(naturalComparatorBy((option) => option.sortValue));
  }, [sources]);
};

export const useAvailableDestinationOptions = (): SortableFilterOption[] => {
  const { destinations } = useDestinationList();

  return useMemo(() => {
    const dedupedDestinationDefinitions = new Map<string, DestinationRead>();
    destinations.forEach((destination) => {
      if (!dedupedDestinationDefinitions.has(destination.destinationDefinitionId)) {
        dedupedDestinationDefinitions.set(destination.destinationDefinitionId, destination);
      }
    });
    return [
      {
        label: (
          <Text bold color="grey">
            <FormattedMessage id="tables.connections.filters.destination.all" />
          </Text>
        ),
        value: null,
        sortValue: "",
      },
      ...Array.from(dedupedDestinationDefinitions).map(([, destination]) => {
        return {
          label: (
            <FlexContainer gap="sm" alignItems="center" as="span">
              <FlexItem>
                <ConnectorIcon icon={destination.icon} />
              </FlexItem>
              <FlexItem>
                <Text size="sm">{destination.destinationName}</Text>
              </FlexItem>
            </FlexContainer>
          ),
          value: destination.destinationDefinitionId,
          sortValue: destination.destinationName,
        };
      }),
    ].sort(naturalComparatorBy((option) => option.sortValue));
  }, [destinations]);
};
