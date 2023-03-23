import { FormattedMessage } from "react-intl";

import { ConnectionSyncButtons } from "components/connection/ConnectionSync/ConnectionSyncButtons";
import { FlexContainer } from "components/ui/Flex";

import styles from "./StreamStatusHeader.module.scss";

export const StreamStatusHeader: React.FC<{ streamCount: number }> = ({ streamCount }) => {
  return (
    <FlexContainer justifyContent="space-between">
      <FormattedMessage id="connection.stream.status.title" />
      <ConnectionSyncButtons
        buttonText={<FormattedMessage id="connection.stream.status.table.syncButton" values={{ streamCount }} />}
        variant="secondary"
        buttonClassName={styles.syncButton}
      />
    </FlexContainer>
  );
};
