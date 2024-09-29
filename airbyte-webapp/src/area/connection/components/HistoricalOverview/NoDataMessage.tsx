import { FormattedMessage } from "react-intl";

import { useConnectionSyncContext } from "components/connection/ConnectionSync/ConnectionSyncContext";
import { EmptyState } from "components/EmptyState";
import { Button } from "components/ui/Button";

import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";

export const NoDataMessage: React.FC = () => {
  const { mode } = useConnectionFormService();
  const { syncConnection, isSyncConnectionAvailable } = useConnectionSyncContext();

  return (
    <EmptyState
      icon="chart"
      text={<FormattedMessage id="connection.overview.graph.noData" />}
      button={
        <Button
          variant="primary"
          type="button"
          onClick={syncConnection}
          disabled={mode === "readonly" || !isSyncConnectionAvailable}
        >
          <FormattedMessage id="connection.overview.graph.noData.button" />
        </Button>
      }
    />
  );
};
