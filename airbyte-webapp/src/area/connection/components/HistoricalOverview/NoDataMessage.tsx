import { FormattedMessage } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import styles from "./NoDataMessage.module.scss";

export const NoDataMessage: React.FC = () => (
  <FlexContainer className={styles.minHeight} alignItems="center" justifyContent="center">
    <Text size="lg">
      <FormattedMessage id="connection.overview.graph.noData" />
    </Text>
  </FlexContainer>
);
