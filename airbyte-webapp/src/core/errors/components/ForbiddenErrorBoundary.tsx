import React from "react";
import { FormattedMessage } from "react-intl";
import { useLocation } from "react-use";
import { LocationSensorState } from "react-use/lib/useLocation";

import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Link } from "components/ui/Link";
import { Text } from "components/ui/Text";

import { HttpError } from "core/api";

import styles from "./ForbiddenErrorBoundary.module.scss";

interface ForbiddenErrorBoundaryState {
  error?: Error;
}

interface ForbiddenErrorBoundaryHookProps {
  location: LocationSensorState;
}

const WORKSPACE_API_REQUESTS = ["/api/v1/workspaces/get", "/api/v1/workspaces/state"];

export const ForbiddenErrorBoundaryView: React.FC = () => {
  return (
    <FlexContainer className={styles.content} direction="column" justifyContent="center" alignItems="center" gap="md">
      <FlexContainer direction="column" alignItems="center" justifyContent="center">
        <FlexContainer className={styles.error__icon} justifyContent="center" alignItems="center">
          <Icon type="lock" size="lg" />
        </FlexContainer>
        <Text size="lg">
          <FormattedMessage id="errors.forbidden.message" />
        </Text>
      </FlexContainer>
      <Link data-testid="forbidden-error-boundary-button" variant="button" to="/workspaces">
        <FormattedMessage id="errors.forbidden.goBack" />
      </Link>
    </FlexContainer>
  );
};

class ForbiddenErrorBoundaryComponent extends React.Component<
  React.PropsWithChildren<ForbiddenErrorBoundaryHookProps>,
  ForbiddenErrorBoundaryState
> {
  state: ForbiddenErrorBoundaryState = { error: undefined };

  static getDerivedStateFromError(error: unknown): ForbiddenErrorBoundaryState {
    const isWorkspaceError =
      error instanceof HttpError &&
      error.status === 403 &&
      WORKSPACE_API_REQUESTS.some((url) => error.request.url.includes(url));
    if (isWorkspaceError) {
      return { error };
    }
    // Let other errors bubble up to higher error boundaries
    throw error;
  }

  override componentDidUpdate(prevProps: ForbiddenErrorBoundaryHookProps) {
    // Clear out the error in case the user navigates to another part of the app
    if (this.props.location !== prevProps.location) {
      this.setState({ error: undefined });
    }
  }

  override render(): React.ReactNode {
    const { error } = this.state;

    if (error) {
      // Render a custom 403 error within the CloudMainView layout
      return <ForbiddenErrorBoundaryView />;
    }

    return this.props.children;
  }
}

export const ForbiddenErrorBoundary: React.FC<React.PropsWithChildren> = ({ children }) => {
  const location = useLocation();

  return <ForbiddenErrorBoundaryComponent location={location}>{children}</ForbiddenErrorBoundaryComponent>;
};
