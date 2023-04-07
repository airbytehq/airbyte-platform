import React from "react";
import { FormattedMessage } from "react-intl";
import { useQueryErrorResetBoundary } from "react-query";
import { NavigateFunction, useNavigate } from "react-router-dom";
import { useLocation } from "react-use";
import { LocationSensorState } from "react-use/lib/useLocation";

import LoadingPage from "components/LoadingPage";

import { isFormBuildError } from "core/form/FormBuildError";
import { isVersionError } from "core/request/VersionError";
import { TrackErrorFn, useAppMonitoringService } from "hooks/services/AppMonitoringService";
import { ErrorOccurredView } from "views/common/ErrorOccurredView";
import { ResourceNotFoundErrorBoundary } from "views/common/ResorceNotFoundErrorBoundary";
import { StartOverErrorView } from "views/common/StartOverErrorView";

import { ServerUnavailableView } from "./ServerUnavailableView";

interface ApiErrorBoundaryState {
  errorId?: string;
  message?: string;
  didRetry?: boolean;
  retryDelay?: number;
  waitForRetry?: boolean;
}

enum ErrorId {
  VersionMismatch = "version.mismatch",
  FormBuild = "form.build",
  ServerUnavailable = "server.unavailable",
  FetchModuleError = "module.fetch",
  UnknownError = "unknown",
}

interface ApiErrorBoundaryHookProps {
  location: LocationSensorState;
  onRetry?: () => void;
  navigate: NavigateFunction;
  trackError: TrackErrorFn;
}

interface ApiErrorBoundaryProps {
  onError?: (errorId?: string) => void;
}

const MODULE_FETCH_ERROR_KEY = "airbyte:lastModuleFetchError";
const RETRY_DELAY = 2500;

class ApiErrorBoundaryComponent extends React.Component<
  React.PropsWithChildren<ApiErrorBoundaryHookProps & ApiErrorBoundaryProps>,
  ApiErrorBoundaryState
> {
  state: ApiErrorBoundaryState = {
    retryDelay: RETRY_DELAY,
  };

  static getDerivedStateFromError(error: { message: string; status?: number; __type?: string }): ApiErrorBoundaryState {
    if (error.message.includes("Failed to fetch dynamically imported module")) {
      const lastModuleFetchError = Number(sessionStorage.getItem(MODULE_FETCH_ERROR_KEY) || "0");
      if (!lastModuleFetchError || Date.now() - lastModuleFetchError >= 10_000) {
        // The last module fetch error is more than 10s ago, i.e. we assume we're not in the direct reload of the previous module fetch error
        // thus we're trying to reload the page, since likely the webapp was just updated.
        console.warn("Reloading the webapp due to a failed module fetch.");
        sessionStorage.setItem(MODULE_FETCH_ERROR_KEY, String(Date.now()));
        window.location.reload();
        return { waitForRetry: true };
      }

      console.error("Got a module fetch error but retried recently, so going to show it.");
      return { errorId: ErrorId.FetchModuleError, message: "An error occured loading parts of the webapp." };
    }

    // Update state so the next render will show the fallback UI.
    if (isVersionError(error)) {
      return { errorId: ErrorId.VersionMismatch, message: error.message };
    }

    if (isFormBuildError(error)) {
      return { errorId: ErrorId.FormBuild, message: error.message };
    }

    const isNetworkBoundaryMessage = error.message === "Failed to fetch";
    const is502 = error.status === 502;

    if (isNetworkBoundaryMessage || is502) {
      return { errorId: ErrorId.ServerUnavailable, didRetry: false };
    }

    return { errorId: ErrorId.UnknownError, didRetry: false };
  }

  componentDidUpdate(prevProps: ApiErrorBoundaryHookProps) {
    const { location } = this.props;

    if (location !== prevProps.location) {
      this.setState({ errorId: undefined, didRetry: false });
      this.props.onError?.(undefined);
    } else {
      this.props.onError?.(this.state.errorId);
    }
  }

  componentDidCatch(error: { message: string; status?: number; __type?: string }) {
    if (isFormBuildError(error)) {
      this.props.trackError(error, {
        id: "formBuildError",
        connectorDefinitionId: error.connectorDefinitionId,
      });
    }
  }

  retry = () => {
    this.setState((state) => ({
      didRetry: true,
      errorId: undefined,
      retryDelay: Math.round((state?.retryDelay || RETRY_DELAY) * 1.2),
    }));
    this.props.onRetry?.();
  };

  render(): React.ReactNode {
    const { navigate, children } = this.props;
    const { errorId, didRetry, message, retryDelay, waitForRetry } = this.state;

    if (waitForRetry) {
      return <LoadingPage />;
    }

    if (errorId === ErrorId.VersionMismatch) {
      return <ErrorOccurredView message={message} />;
    }

    if (errorId === ErrorId.FormBuild) {
      return (
        <ErrorOccurredView
          message={
            <>
              <FormattedMessage id={message} />
              <br />
              <FormattedMessage id="errorView.upgradeConnectors" />
            </>
          }
          docLink="https://docs.airbyte.com/connector-development/connector-specification-reference/#airbyte-modifications-to-jsonschema"
        />
      );
    }

    if (errorId === ErrorId.ServerUnavailable && !didRetry) {
      return <ServerUnavailableView retryDelay={retryDelay || RETRY_DELAY} onRetryClick={this.retry} />;
    }

    return !errorId ? (
      <ResourceNotFoundErrorBoundary errorComponent={<StartOverErrorView />}>{children}</ResourceNotFoundErrorBoundary>
    ) : (
      <ErrorOccurredView
        message={message || <FormattedMessage id="errorView.unknownError" />}
        ctaButtonText={<FormattedMessage id="ui.goBack" />}
        onCtaButtonClick={() => {
          navigate("..");
        }}
      />
    );
  }
}

export const ApiErrorBoundary: React.FC<React.PropsWithChildren<ApiErrorBoundaryProps>> = ({ children, ...props }) => {
  const { reset } = useQueryErrorResetBoundary();
  const location = useLocation();
  const navigate = useNavigate();
  const { trackError } = useAppMonitoringService();

  return (
    <ApiErrorBoundaryComponent
      {...props}
      location={location}
      navigate={navigate}
      onRetry={reset}
      trackError={trackError}
    >
      {children}
    </ApiErrorBoundaryComponent>
  );
};
