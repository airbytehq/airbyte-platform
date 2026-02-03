import React from "react";
import { FormattedMessage } from "react-intl";

import { Text } from "components/ui/Text";

interface NumberOfConnectionsCellProps {
  numConnections?: number;
  enabled: boolean;
}

export const NumberOfConnectionsCell: React.FC<NumberOfConnectionsCellProps> = ({ numConnections, enabled }) => {
  return (
    <Text size="sm" color={enabled ? undefined : "grey300"}>
      {numConnections && numConnections > 0 ? (
        <FormattedMessage id="tables.connectionCount" values={{ num: numConnections }} />
      ) : (
        <FormattedMessage id="general.dash" />
      )}
    </Text>
  );
};
