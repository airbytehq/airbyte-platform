import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
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
  const { schemaRefreshing } = useConnectionEditService();

  const { hasBreakingSchemaChange } = useSchemaChanges(schemaChange);

  if (!allowAutoDetectSchema || !hasBreakingSchemaChange || schemaHasBeenRefreshed || schemaRefreshing) {
    return <>{children}</>;
  }

  return (
    <div className={styles.schemaBreakingChanges} data-testid="schemaChangesBackdrop">
      <FlexContainer justifyContent="center" alignItems="center" className={styles.schemaBreakingChanges__backdrop}>
        <FlexContainer
          justifyContent="center"
          alignItems="center"
          direction="column"
          gap="lg"
          className={styles.schemaBreakingChanges__message}
        >
          <Text align="center" size="lg">
            <FormattedMessage id="connectionForm.schemaChangesBackdrop.message" />
          </Text>
          <Button variant="primaryDark" type="button" onClick={refreshSchema}>
            <FormattedMessage id="connection.schemaChange.reviewAction" />
          </Button>
        </FlexContainer>
      </FlexContainer>
      {children}
    </div>
  );
};
