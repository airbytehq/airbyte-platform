import { useCallback, useMemo } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import Logs from "components/Logs";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { Collapsible } from "components/ui/Collapsible";
import { CopyButton } from "components/ui/CopyButton";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Separator } from "components/ui/Separator";
import { Text } from "components/ui/Text";

import { useGetAllExperiments } from "hooks/services/Experiment";

import styles from "./ErrorDetails.module.scss";
import octavia from "./pixel-octavia.png";
import { I18nError } from "../I18nError";

type FullStoryGlobal = (method: "getSession", options: { format: "url.now" }) => string;

interface ErrorDetailsProps {
  error: Error;
}

const jsonReplacer = (_: string, value: unknown) => (typeof value === "function" ? `[Function ${value.name}]` : value);

export const ErrorDetails: React.FC<ErrorDetailsProps> = ({ error }) => {
  const { formatMessage } = useIntl();
  const getAllExperiments = useGetAllExperiments();
  const getErrorDetails = useCallback(
    () =>
      JSON.stringify(
        {
          url: window.location.href,
          airbyteVersion: process.env.REACT_APP_VERSION,
          errorType: error.name,
          errorConstructor: error.constructor.name,
          error,
          stacktrace: error.stack,
          userAgent: navigator.userAgent,
          // If fullstory is loaded add the current session recording link
          fullStory: (window as { FS?: FullStoryGlobal }).FS?.("getSession", { format: "url.now" }),
          featureFlags: getAllExperiments(),
        },
        jsonReplacer,
        2
      ),
    [error, getAllExperiments]
  );

  const details = useMemo(() => [`// ${error.name}`, ...JSON.stringify(error, jsonReplacer, 2).split("\n")], [error]);

  return (
    <div className={styles.error}>
      <Card className={styles.error__card} bodyClassName={styles.error__cardBody} dataTestId="errorDetails">
        <FlexContainer>
          <img src={octavia} alt="" className={styles.error__octavia} />
          <FlexItem grow>
            <FlexContainer direction="column" gap="sm">
              <FlexContainer alignItems="center" gap="sm">
                <Text size="lg" color="red" bold>
                  <FormattedMessage id="errors.title" />
                </Text>
              </FlexContainer>
              <Text size="lg">{error instanceof I18nError ? error.translate(formatMessage) : error.message}</Text>
            </FlexContainer>
          </FlexItem>
        </FlexContainer>
        <Separator />
        <Collapsible
          label="Error details"
          className={styles.error__collapsible}
          noBodyPadding
          initiallyOpen={false}
          type="inline"
        >
          <Logs logsArray={details} />
        </Collapsible>
        {error.stack && (
          <Collapsible
            label="Stack trace"
            className={styles.error__collapsible}
            noBodyPadding
            initiallyOpen={false}
            type="inline"
          >
            <Logs logsArray={error.stack.split("\n")} />
          </Collapsible>
        )}
        <Box pt="lg">
          <FlexContainer justifyContent="space-between">
            <Button variant="secondary" icon="rotate" onClick={() => window.location.reload()}>
              <FormattedMessage id="errors.reload" />
            </Button>
            <CopyButton content={getErrorDetails}>
              <FormattedMessage id="errors.copyDetails" />
            </CopyButton>
          </FlexContainer>
        </Box>
      </Card>
    </div>
  );
};
