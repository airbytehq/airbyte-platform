import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { Text } from "components/ui/Text";

import { FeatureItem, useFeature } from "core/services/features";
import { useSchemaChanges } from "hooks/connection/useSchemaChanges";
import { useConnectionEditService } from "hooks/services/ConnectionEdit/ConnectionEditService";
import { useConnectionFormService } from "hooks/services/ConnectionForm/ConnectionFormService";

import styles from "./SchemaChangeBackdrop.module.scss";

export const SchemaChangeBackdrop: React.FC<React.PropsWithChildren<unknown>> = ({ children }) => {
  const allowAutoDetectSchema = useFeature(FeatureItem.AllowAutoDetectSchema);

  const {
    schemaHasBeenRefreshed,
    connection: { schemaChange },
  } = useConnectionEditService();

  const { refreshSchema } = useConnectionFormService();

  const { hasBreakingSchemaChange } = useSchemaChanges(schemaChange);

  if (!allowAutoDetectSchema || !hasBreakingSchemaChange || schemaHasBeenRefreshed) {
    return <>{children}</>;
  }

  return (
    <div className={styles.schemaChangeBackdropContainer} data-testid="schemaChangesBackdrop">
      <div className={styles.backdrop}>
        <div className={styles.contentContainer}>
          <Text align="center" size="lg">
            <FormattedMessage id="connectionForm.schemaChangesBackdrop.message" />
          </Text>
          <Button variant="primaryDark" onClick={refreshSchema}>
            <FormattedMessage id="connection.schemaChange.reviewAction" />
          </Button>
        </div>
      </div>
      {children}
    </div>
  );
};
