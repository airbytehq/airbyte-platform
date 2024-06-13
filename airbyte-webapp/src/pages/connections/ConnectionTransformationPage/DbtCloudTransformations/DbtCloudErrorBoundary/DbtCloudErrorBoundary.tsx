import React from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Text } from "components/ui/Text";

import { TrackErrorFn } from "hooks/services/AppMonitoringService";

const DbtCloudErrorCard: React.FC<{ displayMessage?: string | null }> = ({ displayMessage }) => {
  const { formatMessage } = useIntl();

  return (
    <Card title={formatMessage({ id: "connection.dbtCloudJobs.cardTitle" })}>
      <FlexContainer alignItems="center" justifyContent="center">
        <Text align="center">
          {displayMessage ? (
            <FormattedMessage id="connection.dbtCloudJobs.dbtError" values={{ displayMessage }} />
          ) : (
            <FormattedMessage id="connection.dbtCloudJobs.genericError" />
          )}
        </Text>
      </FlexContainer>
    </Card>
  );
};

interface DbtCloudErrorBoundaryProps {
  trackError: TrackErrorFn;
  workspaceId: string;
}

export class DbtCloudErrorBoundary extends React.Component<React.PropsWithChildren<DbtCloudErrorBoundaryProps>> {
  state = { error: null, displayMessage: null };

  /**
   * @author: Alex Birdsall
   * TODO: parse the error to determine if the source was the upstream network call to
   * the dbt Cloud API. If it is, extract the `user_message` field from dbt's error
   * response for display to user; if not, provide a more generic error message. If the
   * error was *definitely* not related to the dbt Cloud API, consider reraising it.
   */
  static getDerivedStateFromError(error: Error) {
    /**
     * @author: Alex Birdsall
     *  TODO: I'm pretty sure I did not correctly mock the exact error response format.
     */
    // eslint-disable-next-line
    const displayMessage = (error?.message as any)?.status?.user_message;
    return { error, displayMessage };
  }

  componentDidCatch(error: Error) {
    const { trackError, workspaceId } = this.props;
    trackError(error, { workspaceId, errorBoundary: this.constructor.name });
  }

  render() {
    const { error, displayMessage } = this.state;
    if (error) {
      return <DbtCloudErrorCard displayMessage={displayMessage} />;
    }

    return this.props.children;
  }
}
