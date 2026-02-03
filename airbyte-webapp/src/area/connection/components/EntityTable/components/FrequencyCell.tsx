import { CellContext, ColumnDefTemplate } from "@tanstack/react-table";
import classNames from "classnames";
import React from "react";
import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { Link } from "components/ui/Link";
import { Text } from "components/ui/Text";

import { useCurrentWorkspaceLink } from "area/workspace/utils";
import { ConnectionScheduleData, ConnectionScheduleType } from "core/api/types/AirbyteClient";
import { RoutePaths } from "pages/routePaths";

import styles from "./FrequencyCell.module.scss";
import { ConnectionTableDataItem } from "../types";

export const FrequencyCell: ColumnDefTemplate<
  CellContext<ConnectionTableDataItem, ConnectionScheduleData | undefined>
> = (props) => {
  const value = props.cell.getValue();
  const enabled = props.row.original.enabled;
  const scheduleType = props.row.original.scheduleType;
  const createLink = useCurrentWorkspaceLink();

  let body: React.ReactNode = null;

  if (scheduleType === ConnectionScheduleType.cron || scheduleType === ConnectionScheduleType.manual) {
    body = (
      <Text className={classNames(styles.text, { [styles.enabled]: enabled })} size="sm">
        <FormattedMessage id={`frequency.${scheduleType}`} />
      </Text>
    );
  } else {
    body = (
      <Text className={classNames(styles.text, { [styles.enabled]: enabled })} size="sm">
        <FormattedMessage
          id={`frequency.${value?.basicSchedule?.timeUnit ?? "manual"}`}
          values={{ value: value?.basicSchedule?.units }}
        />
      </Text>
    );
  }

  return (
    <Box px="lg" py="md">
      <Link to={createLink(`/${RoutePaths.Connections}/${props.row.original.connectionId}`)} variant="primary">
        {body}
      </Link>
    </Box>
  );
};
