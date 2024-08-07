import React from "react";
import { FormattedMessage } from "react-intl";

import { Message } from "components/ui/Message";

import { FormBuildError, isFormBuildError } from "core/form/FormBuildError";
import { trackError } from "core/utils/datadog";

import { BuilderState } from "../types";

interface ApiErrorBoundaryState {
  error?: string | FormBuildError;
}

interface ApiErrorBoundaryProps {
  closeAndSwitchToYaml: () => void;
  currentMode: BuilderState["mode"];
}

export class TestingValuesMenuErrorBoundaryComponent extends React.Component<
  React.PropsWithChildren<ApiErrorBoundaryProps>,
  ApiErrorBoundaryState
> {
  componentDidCatch(error: { message: string; status?: number; __type?: string }): void {
    if (isFormBuildError(error)) {
      trackError(error, {
        id: "formBuildError",
        connectorDefinitionId: error.connectorDefinitionId,
        errorBoundary: this.constructor.name,
      });
    } else {
      // We don't want to handle anything but FormBuildErrors here
      throw error;
    }
  }

  state: ApiErrorBoundaryState = {};

  static getDerivedStateFromError(error: { message: string; __type?: string }): ApiErrorBoundaryState {
    if (isFormBuildError(error)) {
      return { error };
    }

    return { error: error.message };
  }
  render(): React.ReactNode {
    const { children, currentMode, closeAndSwitchToYaml } = this.props;
    const { error } = this.state;

    if (!error) {
      return children;
    }
    return (
      <Message
        text={
          <>
            <FormattedMessage
              id="connectorBuilder.inputsError"
              values={{ error: typeof error === "string" ? error : <FormattedMessage id={error.message} /> }}
            />{" "}
            <a
              target="_blank"
              href="https://docs.airbyte.com/connector-development/connector-specification-reference"
              rel="noreferrer"
            >
              <FormattedMessage id="connectorBuilder.inputsErrorDocumentation" />
            </a>
          </>
        }
        type="error"
        actionBtnText={
          currentMode === "ui" ? (
            <FormattedMessage id="connectorBuilder.goToYaml" />
          ) : (
            <FormattedMessage id="connectorBuilder.close" />
          )
        }
        onAction={closeAndSwitchToYaml}
      />
    );
  }
}
