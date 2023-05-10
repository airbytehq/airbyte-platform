import classNames from "classnames";
import React, { PropsWithChildren } from "react";
import { FormattedMessage } from "react-intl";

import { Message } from "components/ui/Message";
import { Text } from "components/ui/Text";

import { useShowEnrollmentModal } from "./EnrollmentModal";
import { useFreeConnectorProgram } from "./hooks/useFreeConnectorProgram";
import styles from "./InlineEnrollmentCallout.module.scss";

export const EnrollLink: React.FC<PropsWithChildren<unknown>> = ({ children }) => {
  const { showEnrollmentModal } = useShowEnrollmentModal();

  return (
    <button onClick={showEnrollmentModal} className={styles.enrollLink}>
      {children}
    </button>
  );
};

interface InlineEnrollmentCalloutProps {
  withBottomMargin?: boolean;
}

export const InlineEnrollmentCallout: React.FC<InlineEnrollmentCalloutProps> = ({ withBottomMargin }) => {
  const { userDidEnroll, enrollmentStatusQuery } = useFreeConnectorProgram();
  const { showEnrollmentUi } = enrollmentStatusQuery.data || {};

  if (userDidEnroll || !showEnrollmentUi) {
    return null;
  }

  return (
    <Message
      type="info"
      text={
        <Text size="sm">
          <FormattedMessage
            id="freeConnectorProgram.joinTheFreeConnectorProgram"
            values={{
              enrollLink: (content: React.ReactNode) => <EnrollLink>{content}</EnrollLink>,
            }}
          />
        </Text>
      }
      className={classNames(styles.container, { [styles.withBottomMargin]: withBottomMargin })}
    />
  );
};

export default InlineEnrollmentCallout;
