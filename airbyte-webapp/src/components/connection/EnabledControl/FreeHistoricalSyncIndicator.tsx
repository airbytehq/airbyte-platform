import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { Text } from "components/ui/Text";
import { Tooltip } from "components/ui/Tooltip";

import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";
import { useExperiment } from "hooks/services/Experiment";

export const FreeHistoricalSyncIndicator: React.FC = () => {
  const isFreeHistoricalSyncEnabled = useExperiment("billing.early-sync-enabled");
  const {
    connection: { createdAt: connectionCreatedAt },
  } = useConnectionFormService();

  // 7 days of free syncs minus the days since connectionCreatedAt... or 0 in the case that doesn't exist
  const daysRemaining = 7 - Math.floor((Date.now() / 1000 - (connectionCreatedAt || 0)) / (24 * 60 * 60));

  if (daysRemaining <= 0 || !isFreeHistoricalSyncEnabled) {
    return null;
  }

  return (
    <Tooltip
      control={
        <Text color="grey400" size="md">
          {daysRemaining === 7 ? (
            <FormattedMessage id="connection.freeHistoricalSyncs.message.initial" />
          ) : daysRemaining > 0 ? (
            <FormattedMessage id="connection.freeHistoricalSyncs.message" values={{ count: daysRemaining }} />
          ) : null}
          <Box as="span" pl="sm">
            <FormattedMessage id="general.unicodeBullet" />
          </Box>
        </Text>
      }
    >
      <FormattedMessage id="connection.freeHistoricalSyncs.tooltip" />
    </Tooltip>
  );
};
