import React from "react";
import { FormattedMessage } from "react-intl";

import { ConnectorIcon } from "components/ConnectorIcon";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import {
  DestinationDefinitionId,
  SourceDefinitionId,
  WebBackendConnectionListItem,
} from "core/api/types/AirbyteClient";
import { naturalComparatorBy } from "core/utils/objects";

import { connectionStatColors, SummaryKey } from "../ConnectionsSummary";

type filterIconType = "successFilled" | "errorFilled" | "sync" | "pauseFilled";

const generateStatusFilterOption = (value: SummaryKey, id: string, iconType: filterIconType, color: SummaryKey) => ({
  label: (
    <FlexContainer gap="sm" alignItems="center">
      <FlexItem>
        <Text color={connectionStatColors[color]} as="span">
          <Icon type={iconType} size="md" />
        </Text>
      </FlexItem>
      <FlexItem>
        <Text color="grey" bold as="span">
          &nbsp; <FormattedMessage id={id} />
        </Text>
      </FlexItem>
    </FlexContainer>
  ),
  value,
});

const generateStateFilterOption = (value: "enabled" | "disabled" | "all", id: string) => ({
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
  generateStatusFilterOption("healthy", "tables.connections.filters.status.healthy", "successFilled", "healthy"),
  generateStatusFilterOption("failed", "tables.connections.filters.status.failed", "errorFilled", "failed"),
  generateStatusFilterOption("running", "tables.connections.filters.status.running", "sync", "running"),
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
  generateStateFilterOption("enabled", "tables.connections.filters.state.enabled"),
  generateStateFilterOption("disabled", "tables.connections.filters.state.disabled"),
];

interface FilterOption {
  label: React.ReactNode;
  value: string | null;
}

type SortableFilterOption = FilterOption & { sortValue: string };

export const getAvailableSourceOptions = (
  connections: WebBackendConnectionListItem[],
  selectedDestination: DestinationDefinitionId | null
) =>
  connections
    .reduce<{
      foundSourceIds: Set<string>;
      options: SortableFilterOption[];
    }>(
      (acc, connection) => {
        const {
          source: { sourceName, sourceDefinitionId, icon },
          destination: { destinationDefinitionId },
        } = connection;

        if (
          !acc.foundSourceIds.has(sourceDefinitionId) &&
          (!selectedDestination || destinationDefinitionId === selectedDestination)
        ) {
          acc.foundSourceIds.add(sourceDefinitionId);
          acc.options.push({
            label: (
              <FlexContainer gap="sm" alignItems="center" as="span">
                <FlexItem>
                  <ConnectorIcon icon={icon} />
                </FlexItem>
                <FlexItem>
                  <Text size="sm">{sourceName}</Text>
                </FlexItem>
              </FlexContainer>
            ),
            value: sourceDefinitionId,
            sortValue: sourceName,
          });
        }
        return acc;
      },
      {
        foundSourceIds: new Set(),
        options: [
          {
            label: (
              <Text bold color="grey">
                <FormattedMessage id="tables.connections.filters.source.all" />
              </Text>
            ),
            value: null,
            sortValue: "",
          },
        ],
      }
    )
    .options.sort(naturalComparatorBy((option) => option.sortValue));

export const getAvailableDestinationOptions = (
  connections: WebBackendConnectionListItem[],
  selectedSource?: SourceDefinitionId | null
) =>
  connections
    .reduce<{
      foundDestinationIds: Set<string>;
      options: SortableFilterOption[];
    }>(
      (acc, connection) => {
        const {
          destination: { destinationName, destinationDefinitionId, icon },
          source: { sourceDefinitionId },
        } = connection;

        if (
          !acc.foundDestinationIds.has(destinationDefinitionId) &&
          (!selectedSource || sourceDefinitionId === selectedSource)
        ) {
          acc.foundDestinationIds.add(connection.destination.destinationDefinitionId);
          acc.options.push({
            label: (
              <FlexContainer gap="sm" alignItems="center" as="span">
                <FlexItem>
                  <ConnectorIcon icon={icon} />
                </FlexItem>
                <FlexItem>
                  <Text size="sm">{destinationName}</Text>
                </FlexItem>
              </FlexContainer>
            ),
            value: destinationDefinitionId,
            sortValue: destinationName,
          });
        }
        return acc;
      },
      {
        foundDestinationIds: new Set(),
        options: [
          {
            label: (
              <Text bold color="grey">
                <FormattedMessage id="tables.connections.filters.destination.all" />
              </Text>
            ),
            value: null,
            sortValue: "",
          },
        ],
      }
    )
    .options.sort(naturalComparatorBy((option) => option.sortValue));
