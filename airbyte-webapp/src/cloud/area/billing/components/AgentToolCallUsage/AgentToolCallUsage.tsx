import classNames from "classnames";
import { FormattedMessage, FormattedNumber, useIntl } from "react-intl";

import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { ExternalLink } from "components/ui/Link";
import { LoadingSkeleton } from "components/ui/LoadingSkeleton";
import { Message } from "components/ui/Message";
import { Text } from "components/ui/Text";

import { useFusionUsage } from "core/api/cloud";
import { links } from "core/utils/links";

import styles from "./AgentToolCallUsage.module.scss";

export const AgentToolCallUsage: React.FC = () => {
  const { formatMessage } = useIntl();
  const { data, isEligible, isInitialLoading, isError, refetch } = useFusionUsage();
  if (!isEligible) {
    return null;
  }

  const month = data?.month;
  const usage =
    data?.freeCapsApplicable &&
    month?.used !== null &&
    month?.used !== undefined &&
    month.limit !== null &&
    month.limit > 0
      ? month.used / month.limit
      : null;

  return (
    <Card dataTestId="agent-tool-call-usage">
      <FlexContainer direction="column" gap="lg">
        <Heading as="h2" size="sm">
          <FormattedMessage id="fusion.usage.title" />
        </Heading>
        <Text size="sm" color="grey">
          <FormattedMessage
            id={
              data && !data.freeCapsApplicable ? "fusion.usage.description.notApplicable" : "fusion.usage.description"
            }
            values={{ lnk: (node: React.ReactNode) => <ExternalLink href={links.airbyteMcpDocs}>{node}</ExternalLink> }}
          />
        </Text>
        {isInitialLoading ? (
          <div aria-busy="true" aria-label={formatMessage({ id: "settings.organization.usage.loadingUsageData" })}>
            <LoadingSkeleton />
          </div>
        ) : isError ? (
          <Message
            type="error"
            text={<FormattedMessage id="fusion.usage.error" />}
            onAction={() => void refetch()}
            actionBtnText={<FormattedMessage id="fusion.usage.retry" />}
          />
        ) : data && !data.freeCapsApplicable ? (
          <Text size="sm" color="grey">
            <FormattedMessage id="fusion.usage.notApplicable" />
          </Text>
        ) : month ? (
          <>
            <FlexContainer gap="2xl" wrap="wrap">
              <FlexContainer direction="column" gap="sm">
                <Text size="xs" color="grey">
                  <FormattedMessage id="fusion.usage.used" />
                </Text>
                <Text size="xl" bold data-testid="agent-tool-call-used">
                  {month.used === null ? (
                    <FormattedMessage
                      id={month.limit === null ? "fusion.usage.notTracked" : "fusion.usage.unavailable"}
                    />
                  ) : (
                    <FormattedNumber value={month.used} />
                  )}
                </Text>
              </FlexContainer>
              <FlexContainer direction="column" gap="sm">
                <Text size="xs" color="grey">
                  <FormattedMessage id="fusion.usage.limit" />
                </Text>
                <Text size="xl" bold data-testid="agent-tool-call-limit">
                  {month.limit === null ? (
                    <FormattedMessage id="fusion.usage.unlimited" />
                  ) : (
                    <FormattedNumber value={month.limit} />
                  )}
                </Text>
              </FlexContainer>
              <FlexContainer direction="column" gap="sm">
                <Text size="xs" color="grey">
                  <FormattedMessage id="fusion.usage.percentage" />
                </Text>
                <Text size="xl" bold data-testid="agent-tool-call-percentage">
                  {usage === null ? (
                    <FormattedMessage id="fusion.usage.notAvailable" />
                  ) : (
                    <FormattedNumber value={usage} style="percent" maximumFractionDigits={1} />
                  )}
                </Text>
              </FlexContainer>
            </FlexContainer>
            {usage !== null && (
              <div
                className={styles.usage__track}
                role="progressbar"
                aria-label={formatMessage({ id: "fusion.usage.title" })}
                aria-valuemin={0}
                aria-valuemax={100}
                aria-valuenow={Math.min(100, usage * 100)}
                aria-valuetext={formatMessage(
                  { id: "fusion.usage.progress" },
                  { used: month.used, limit: month.limit }
                )}
              >
                <div
                  className={classNames(styles.usage__fill, { [styles["usage__fill--exhausted"]]: usage >= 1 })}
                  style={{ width: `${Math.min(100, usage * 100)}%` }}
                />
              </div>
            )}
            {month.used === null && month.limit !== null && (
              <FlexContainer alignItems="center">
                <Text size="sm" color="grey">
                  <FormattedMessage id="fusion.usage.counterUnavailable" />
                </Text>
                <Button variant="secondary" size="xs" onClick={() => void refetch()}>
                  <FormattedMessage id="fusion.usage.retry" />
                </Button>
              </FlexContainer>
            )}
          </>
        ) : null}
      </FlexContainer>
    </Card>
  );
};
