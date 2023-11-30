import { Disclosure } from "@headlessui/react";
import classNames from "classnames";
import { useMemo } from "react";
import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Message } from "components/ui/Message";

import { AttemptFailureSummary, FailureType } from "core/request/AirbyteClient";

import styles from "./JobLogsModalFailureMessage.module.scss";

interface JobLogsModalFailureMessageProps {
  failureSummary?: AttemptFailureSummary;
}

export const JobLogsModalFailureMessage: React.FC<JobLogsModalFailureMessageProps> = ({ failureSummary }) => {
  const internalFailureReason = useMemo(() => failureSummary?.failures[0]?.internalMessage, [failureSummary]);

  const externalFailureReason = useMemo(() => failureSummary?.failures[0]?.externalMessage, [failureSummary]);

  const failureToShow = useMemo(
    () =>
      !failureSummary ||
      failureSummary?.failures.some(({ failureType }) => failureType === FailureType.manual_cancellation)
        ? "none"
        : failureSummary?.failures[0]?.internalMessage
        ? "internal"
        : failureSummary?.failures[0]?.externalMessage
        ? "external"
        : "unknown",
    [failureSummary]
  );

  if (failureToShow === "none") {
    return null;
  }

  return (
    <Box px="md" pb="md">
      {failureToShow === "internal" && (
        <Disclosure>
          {({ open }) => (
            <div className={classNames(styles.disclosureContainer, { [styles.disclosureContainer__open]: open })}>
              <Message
                type="error"
                text={
                  <FlexContainer justifyContent="space-between" alignItems="center">
                    <FormattedMessage id="jobHistory.logs.failureReason" values={{ reason: externalFailureReason }} />

                    <Disclosure.Button as={Button}>
                      <FormattedMessage id="jobs.failure.seeMore" />
                    </Disclosure.Button>
                  </FlexContainer>
                }
              />

              <Disclosure.Panel>
                <Box px="md">
                  <div className={styles.internalFailureReason}>{internalFailureReason}</div>
                </Box>
              </Disclosure.Panel>
            </div>
          )}
        </Disclosure>
      )}

      {failureToShow === "external" && (
        <Message
          type="error"
          text={<FormattedMessage id="jobHistory.logs.failureReason" values={{ reason: externalFailureReason }} />}
        />
      )}

      {failureToShow === "unknown" && <Message type="error" text={<FormattedMessage id="errorView.unknown" />} />}
    </Box>
  );
};
