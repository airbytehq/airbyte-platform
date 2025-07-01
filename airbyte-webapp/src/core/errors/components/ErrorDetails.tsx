import { useCallback, useMemo } from "react";
import { FormattedMessage } from "react-intl";

import Logs from "components/Logs";
import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { Collapsible } from "components/ui/Collapsible";
import { CopyButton } from "components/ui/CopyButton";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { ExternalLink } from "components/ui/Link";
import { Separator } from "components/ui/Separator";
import { Text } from "components/ui/Text";

import { HttpProblem } from "core/api";
import { fullStorySessionLink } from "core/utils/fullstory";
import { useGetAllExperiments } from "hooks/services/Experiment";

import styles from "./ErrorDetails.module.scss";
import octavia from "./pixel-octavia.png";
import { useFormatError } from "../formatErrors";

interface ErrorDetailsProps {
  error: Error;
}

const jsonReplacer = (_: string, value: unknown) => (typeof value === "function" ? `[Function ${value.name}]` : value);

export const ErrorDetails: React.FC<ErrorDetailsProps> = ({ error }) => {
  const formatError = useFormatError();
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
          fullStory: fullStorySessionLink(),
          featureFlags: getAllExperiments(),
        },
        jsonReplacer,
        2
      ),
    [error, getAllExperiments]
  );

  const details = useMemo(() => [`// ${error.name}`, ...JSON.stringify(error, jsonReplacer, 2).split("\n")], [error]);

  return (
    <FlexContainer className={styles.wrapper} direction="column" gap="none">
      <FlexContainer className={styles.container} gap="none">
        <FlexContainer
          className={styles.content}
          direction="column"
          justifyContent="center"
          alignItems="center"
          gap="md"
        >
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
                  <Text size="lg">{formatError(error)}</Text>
                  {HttpProblem.isInstanceOf(error) && error.response.documentationUrl && (
                    <Text size="lg">
                      <ExternalLink href={error.response.documentationUrl}>
                        <FlexContainer gap="xs" alignItems="center">
                          <FormattedMessage id="errors.readMore" />
                          <Icon type="share" size="sm" />
                        </FlexContainer>
                      </ExternalLink>
                    </Text>
                  )}
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
        </FlexContainer>
      </FlexContainer>
    </FlexContainer>
  );
};
