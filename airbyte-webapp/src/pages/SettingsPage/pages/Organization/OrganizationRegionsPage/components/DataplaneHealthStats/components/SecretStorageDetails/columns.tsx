import { createColumnHelper } from "@tanstack/react-table";
import { FormattedMessage } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { ConfiguredFor } from "./ConfiguredFor";
import { SecretStorageRow } from "./types";
import { Age } from "../Age";
import { LevelValue } from "../LevelValue";

const columnHelper = createColumnHelper<SecretStorageRow>();

export const columns = [
  columnHelper.display({
    id: "store",
    header: () => (
      <FormattedMessage id="settings.organization.regions.dataplane.diagnostics.secretStorage.details.store" />
    ),
    cell: ({ row: { original: dataItem } }) => (
      <LevelValue level={dataItem.level}>{dataItem.name ?? dataItem.id}</LevelValue>
    ),
  }),
  columnHelper.display({
    id: "configuredFor",
    header: () => (
      <FormattedMessage id="settings.organization.regions.dataplane.diagnostics.secretStorage.details.configuredFor" />
    ),
    cell: ({ row: { original: dataItem } }) => (
      <FlexContainer direction="column" gap="sm">
        <Text as="div" size="sm">
          {dataItem.scope ? (
            <ConfiguredFor scope={dataItem.scope} />
          ) : (
            <FormattedMessage id="settings.organization.regions.dataplane.diagnostics.secretStorage.details.scope.unknown" />
          )}
        </Text>
        <Text size="sm" color="grey400">
          {dataItem.scope?.id}
        </Text>
      </FlexContainer>
    ),
  }),
  columnHelper.display({
    id: "lastChecked",
    header: () => (
      <FormattedMessage id="settings.organization.regions.dataplane.diagnostics.secretStorage.details.lastChecked" />
    ),
    cell: ({ row: { original: dataItem } }) => (
      <Text size="sm">
        <Age age={dataItem.lastChecked} />
      </Text>
    ),
  }),
];
