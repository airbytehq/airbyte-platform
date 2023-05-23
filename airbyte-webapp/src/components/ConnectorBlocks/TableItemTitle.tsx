import React from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { ReleaseStageBadge } from "components/ReleaseStageBadge";
import { Box } from "components/ui/Box";
import { DropdownMenu, DropdownMenuOptionType } from "components/ui/DropdownMenu";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Text } from "components/ui/Text";

import { ReleaseStage } from "core/request/AirbyteClient";
import { useExperiment } from "hooks/services/Experiment";
import { getIcon } from "utils/imageUtils";

import styles from "./TableItemTitle.module.scss";
import { Button } from "../ui/Button";

interface TableItemTitleProps {
  type: "source" | "destination";
  dropdownOptions: DropdownMenuOptionType[];
  onSelect: (data: DropdownMenuOptionType) => void;
  entity: string;
  entityName: string;
  entityIcon?: string;
  releaseStage?: ReleaseStage;
  connectionsCount: number;
}

const TableItemTitle: React.FC<TableItemTitleProps> = ({
  type,
  dropdownOptions,
  onSelect,
  entity,
  entityName,
  entityIcon,
  releaseStage,
  connectionsCount,
}) => {
  const { formatMessage } = useIntl();
  const isNewConnectionFlowEnabled = useExperiment("connection.updatedConnectionFlow", false);
  return (
    <Box px="xl" pt="lg">
      {!isNewConnectionFlowEnabled && (
        <div className={styles.entityInfo}>
          {entityIcon && <div className={styles.entityIcon}>{getIcon(entityIcon)}</div>}
          <div>
            <Heading as="h2">{entityName}</Heading>
            <Text size="lg" bold className={styles.entityType}>
              <span>{entity}</span>
              <ReleaseStageBadge stage={releaseStage} />
            </Text>
          </div>
        </div>
      )}

      <FlexContainer alignItems="center" justifyContent="space-between">
        <Heading as="h3" size="sm">
          <FormattedMessage id="tables.connections.pluralized" values={{ value: connectionsCount }} />
        </Heading>
        <DropdownMenu
          placement="bottom-end"
          options={[
            {
              as: "button",
              className: styles.primary,
              displayName: formatMessage({
                id: `tables.${type}AddNew`,
              }),
            },
            ...dropdownOptions,
          ]}
          onChange={onSelect}
        >
          {() => (
            <Button data-testid={`select-${type}`}>
              <FormattedMessage id={`tables.${type}Add`} />
            </Button>
          )}
        </DropdownMenu>
      </FlexContainer>
    </Box>
  );
};

export default TableItemTitle;
