import { FormattedMessage } from "react-intl";

import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import styles from "./EmptyState.module.scss";

export const EmptyState: React.FC = () => {
  return (
    <FlexContainer alignItems="center" justifyContent="center" direction="column" className={styles.container}>
      <Text>
        <FormattedMessage id="credits.noData" />
      </Text>
    </FlexContainer>
  );
};
