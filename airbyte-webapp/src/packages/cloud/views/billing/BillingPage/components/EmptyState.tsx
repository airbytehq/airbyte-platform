import { FormattedMessage } from "react-intl";

import octavia from "components/illustrations/octavia-pointing.svg";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import styles from "./EmptyState.module.scss";

export const EmptyState: React.FC = () => {
  return (
    <FlexContainer alignItems="center" justifyContent="center" direction="column" className={styles.container}>
      <img alt="No credits data" src={octavia} width={102} />
      <Text>
        <FormattedMessage id="credits.noData" />
      </Text>
    </FlexContainer>
  );
};
