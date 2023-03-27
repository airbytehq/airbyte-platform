import classNames from "classnames";
import { FormattedMessage } from "react-intl";
import { useNavigate } from "react-router-dom";

import { Button } from "components/ui/Button";
import { Text } from "components/ui/Text";

import { useSchemaChanges } from "hooks/connection/useSchemaChanges";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";
import { ConnectionRoutePaths } from "pages/connections/types";

import styles from "./SchemaChangesDetected.module.scss";

export const SchemaChangesDetected: React.FC = () => {
  const {
    connection: { schemaChange },
    schemaRefreshing,
    schemaHasBeenRefreshed,
  } = useConnectionEditService();

  const { hasBreakingSchemaChange, hasNonBreakingSchemaChange } = useSchemaChanges(schemaChange);
  const navigate = useNavigate();

  if (schemaHasBeenRefreshed) {
    return null;
  }

  const onReviewActionButtonClick: React.MouseEventHandler<HTMLButtonElement> = () => {
    navigate(ConnectionRoutePaths.Replication, { state: { triggerRefreshSchema: true } });
  };

  const schemaChangeClassNames = {
    [styles.breaking]: hasBreakingSchemaChange,
    [styles.nonBreaking]: hasNonBreakingSchemaChange,
  };

  return (
    <div className={classNames(styles.container, schemaChangeClassNames)} data-testid="schemaChangesDetected">
      <Text size="lg">
        <FormattedMessage id={`connection.schemaChange.${hasBreakingSchemaChange ? "breaking" : "nonBreaking"}`} />
      </Text>
      <Button
        variant="dark"
        onClick={onReviewActionButtonClick}
        isLoading={schemaRefreshing}
        data-testid="schemaChangesReviewButton"
      >
        <FormattedMessage id="connection.schemaChange.reviewAction" />
      </Button>
    </div>
  );
};
