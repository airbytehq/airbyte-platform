import { SchemaFormControl } from "components/forms/SchemaForm/Controls/SchemaFormControl";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";

import { useConnectorBuilderPermission } from "services/connectorBuilder/ConnectorBuilderStateService";

import styles from "./GlobalConfigView.module.scss";
export const GlobalConfigView: React.FC = () => {
  const permission = useConnectorBuilderPermission();

  return (
    <fieldset className={styles.fieldset} disabled={permission === "readOnly"}>
      <FlexContainer direction="column">
        <Card className={styles.card}>
          <SchemaFormControl path="manifest.check" />
        </Card>
        <Card className={styles.card}>
          <SchemaFormControl path="manifest.concurrency_level" />
        </Card>
        <Card className={styles.card}>
          <SchemaFormControl path="manifest.api_budget" />
        </Card>
      </FlexContainer>
    </fieldset>
  );
};
