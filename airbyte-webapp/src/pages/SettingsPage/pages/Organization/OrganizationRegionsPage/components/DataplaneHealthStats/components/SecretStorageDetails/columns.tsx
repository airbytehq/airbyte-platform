import { createColumnHelper } from "@tanstack/react-table";
import { FormattedMessage } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { ConfiguredFor } from "./ConfiguredFor";
import { SecretStorageRow } from "./types";
import { StatusLevelIcon } from "../StatusCard";

const columnHelper = createColumnHelper<SecretStorageRow>();

export const columns = [
  columnHelper.display({
    id: "store",
    header: () => (
      <FormattedMessage id="settings.organization.regions.dataplane.diagnostics.secretStorage.details.store" />
    ),
    cell: ({ row: { original: dataItem } }) => (
      <FlexContainer alignItems="center" gap="sm">
        <StatusLevelIcon level={dataItem.level} />
        <Text size="sm" bold>
          {dataItem.name ?? dataItem.id}
        </Text>
      </FlexContainer>
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
        <FormattedMessage id={dataItem.lastChecked.messageId} values={dataItem.lastChecked.values} />
      </Text>
    ),
  }),
];
