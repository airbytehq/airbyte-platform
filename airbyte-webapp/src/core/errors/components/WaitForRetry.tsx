import { useEffect, useMemo } from "react";
import { FormattedMessage } from "react-intl";

import { LoadingPage } from "components";
import { Button } from "components/ui/Button";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import styles from "./WaitForRetry.module.scss";

interface WaitForRetryProps {
  retry: () => void;
}

/**
 * The delay in milliseconds before retrying in case we're online when this component is rendered.
 */
const RETRY_WHEN_ONLINE_DELAY = 1500;

export const WaitForRetry: React.FC<WaitForRetryProps> = ({ retry }) => {
  const isOffline = useMemo(() => window.navigator.onLine === false, []);

  useEffect(() => {
    // In case we're offline, wait for us to go online and then retry
    if (isOffline) {
      window.addEventListener("online", retry);
      return () => window.removeEventListener("online", retry);
    }
    // In case we're already online, just wait for a bit before retrying
    const timeoutId = setTimeout(retry, RETRY_WHEN_ONLINE_DELAY);
    return () => clearTimeout(timeoutId);
  }, [isOffline, retry]);

  return (
    <FlexContainer
      className={styles.waitForRetry}
      justifyContent="center"
      alignItems="center"
      direction="column"
      data-testId="waitForRetry"
    >
      <FlexItem grow={false}>
        <LoadingPage />
      </FlexItem>
      <FlexItem grow={false}>
        <Text>
          {isOffline ? <FormattedMessage id="errors.retry.offline" /> : <FormattedMessage id="errors.retry.online" />}
        </Text>
      </FlexItem>
      <FlexItem grow={false}>
        <Button variant="clear" icon="rotate" onClick={retry}>
          <FormattedMessage id="errors.retry.button" />
        </Button>
      </FlexItem>
    </FlexContainer>
  );
};
