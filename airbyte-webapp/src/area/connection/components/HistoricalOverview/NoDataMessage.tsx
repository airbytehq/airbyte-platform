import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { EmptyState } from "components/ui/EmptyState";

import { useConnectionSyncContext } from "area/connection/components/ConnectionSync/ConnectionSyncContext";
import { useCurrentConnection } from "core/api";
import { Intent, useGeneratedIntent } from "core/utils/rbac";

export const NoDataMessage: React.FC = () => {
  const { syncConnection, isSyncConnectionAvailable } = useConnectionSyncContext();
  const canSyncConnection = useGeneratedIntent(Intent.RunAndCancelConnectionSyncAndRefresh);
  const connection = useCurrentConnection();

  return (
    <EmptyState
      icon="chart"
      text={<FormattedMessage id="connection.overview.graph.noData" />}
      button={
        <Button
          variant="primary"
          type="button"
          onClick={syncConnection}
          disabled={connection.status !== "active" || !isSyncConnectionAvailable || !canSyncConnection}
        >
          <FormattedMessage id="connection.overview.graph.noData.button" />
        </Button>
      }
    />
  );
};
