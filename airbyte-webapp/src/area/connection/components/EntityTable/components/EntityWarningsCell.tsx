import { PropsOf } from "@headlessui/react/dist/types";
import { CellContext, ColumnDefTemplate } from "@tanstack/react-table";
import { ReactNode } from "react";
import { FormattedMessage } from "react-intl";

import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Link } from "components/ui/Link";
import { MessageType } from "components/ui/Message";
import { NumberBadge } from "components/ui/NumberBadge";
import { Tooltip } from "components/ui/Tooltip";

import { useCurrentWorkspaceLink } from "area/workspace/utils";
import { SchemaChange, WebBackendConnectionListItem } from "core/api/types/AirbyteClient";
import { getHumanReadableUpgradeDeadline, shouldDisplayBreakingChangeBanner } from "core/domain/connector";
import { FeatureItem, useFeature } from "core/services/features";
import { convertSnakeToCamel } from "core/utils/strings";
import { getBreakingChangeErrorMessage } from "pages/connections/StreamStatusPage/ConnectionStatusMessages";
import { ConnectionRoutePaths, RoutePaths } from "pages/routePaths";

import styles from "./EntityWarningsCell.module.scss";
import { ConnectionTableDataItem } from "../types";

const schemaChangeToMessageType: Record<SchemaChange, MessageType> = {
  breaking: "error",
  non_breaking: "warning",
  no_change: "success",
};

const typetoIcon: Record<MessageType, ReactNode> = {
  warning: <Icon data-testid="entitywarnings-warning" size="sm" type="infoFilled" color="warning" />,
  success: null,
  error: <Icon data-testid="entitywarnings-error" size="sm" type="statusWarning" color="error" />,
  info: null,
};

export const EntityWarningsCell: ColumnDefTemplate<
  CellContext<ConnectionTableDataItem, WebBackendConnectionListItem>
> = (props) => {
  const connection = props.cell.getValue();
  const allowAutoDetectSchema = useFeature(FeatureItem.AllowAutoDetectSchema);
  const connectorBreakingChangeDeadlinesEnabled = useFeature(FeatureItem.ConnectorBreakingChangeDeadlines);
  const createLink = useCurrentWorkspaceLink();

  const {
    connectionId,
    schemaChange,
    sourceActorDefinitionVersion: source,
    destinationActorDefinitionVersion: destination,
  } = connection;

  const warningsToShow: Array<[MessageType, ReactNode, PropsOf<typeof Link> & Record<"data-testid", string>]> = [];

  if (allowAutoDetectSchema && schemaChange === SchemaChange.breaking) {
    warningsToShow.push([
      schemaChangeToMessageType[schemaChange],
      <FormattedMessage id={`connection.schemaChange.${convertSnakeToCamel(schemaChange)}`} />,
      {
        to: `${connectionId}/${ConnectionRoutePaths.Replication}`,
        "data-testid": `link-replication-${connectionId}`,
      },
    ]);
  }

  if (shouldDisplayBreakingChangeBanner(source)) {
    const { errorMessageId, errorType } = getBreakingChangeErrorMessage(
      source,
      connectorBreakingChangeDeadlinesEnabled
    );

    warningsToShow.push([
      errorType,
      <FormattedMessage
        id={errorMessageId}
        values={{
          actor_name: connection.source.name,
          actor_definition_name: connection.source.sourceName,
          actor_type: "source",
          upgrade_deadline: getHumanReadableUpgradeDeadline(source),
        }}
      />,
      {
        to: createLink(`/${RoutePaths.Source}/${connection.source.sourceId}`),
        "data-testid": `link-source-${connectionId}`,
      },
    ]);
  }

  if (shouldDisplayBreakingChangeBanner(destination)) {
    const { errorMessageId, errorType } = getBreakingChangeErrorMessage(
      destination,
      connectorBreakingChangeDeadlinesEnabled
    );
    warningsToShow.push([
      errorType,
      <FormattedMessage
        id={errorMessageId}
        values={{
          actor_name: connection.destination.name,
          actor_definition_name: connection.destination.destinationName,
          actor_type: "destination",
          upgrade_deadline: getHumanReadableUpgradeDeadline(destination),
        }}
      />,
      {
        to: createLink(`/${RoutePaths.Destination}/${connection.destination.destinationId}`),
        "data-testid": `link-source-${connectionId}`,
      },
    ]);
  }

  if (warningsToShow.length === 0) {
    return null;
  }

  if (warningsToShow.length === 1) {
    const [messageType, messageNode, linkProps] = warningsToShow[0];
    return (
      <Tooltip
        placement="bottom"
        containerClassName={styles.tooltipContainer}
        control={<Link {...linkProps}>{typetoIcon[messageType]}</Link>}
      >
        {messageNode}
      </Tooltip>
    );
  }

  warningsToShow.sort(([a], [b]) => {
    if (a === "error" && b !== "error") {
      return -1;
    }
    if (b === "error" && a !== "error") {
      return 1;
    }
    return 0;
  });

  const highestMessageType = warningsToShow[0][0];

  return (
    <Tooltip
      placement="bottom"
      containerClassName={styles.tooltipContainer}
      control={
        <NumberBadge
          small
          inverse
          value={warningsToShow.length}
          color={highestMessageType === "warning" ? "yellow" : "red"}
        />
      }
    >
      <FlexContainer direction="column" justifyContent="flex-start">
        {warningsToShow.map(([messageType, messageNode], idx) => (
          <FlexContainer key={idx}>
            <FlexItem>{typetoIcon[messageType]}</FlexItem>
            <FlexItem>{messageNode}</FlexItem>
          </FlexContainer>
        ))}
      </FlexContainer>
    </Tooltip>
  );
};
