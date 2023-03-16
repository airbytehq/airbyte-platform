import { FormattedMessage } from "react-intl";

import octavia from "components/illustrations/octavia-pointing.svg";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import styles from "./EmptyState.module.scss";

export const EmptyState: React.FC = () => {
  return (
    <Card className={styles.card}>
      <FlexContainer alignItems="center" justifyContent="center" direction="column" className={styles.container}>
        <img className={styles.logo} alt="No credits data" src={octavia} width={102} />
        <Text>
          <FormattedMessage id="credits.noData" />
        </Text>
      </FlexContainer>
    </Card>
  );
};
