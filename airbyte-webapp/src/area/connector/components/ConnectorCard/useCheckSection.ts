import { useIntl } from "react-intl";

import { ConnectTarget, ConnectorFormValues } from "area/connector/components/ConnectorForm";
import { useConnectCommand, useTestConnectorCommand } from "core/api";
import { GetConnectCommandOutput200ErrorCode } from "core/api/types/AirbyteClient";
import { Connector, ConnectorDefinition } from "core/domain/connector";

const CONNECT_FAILURE_MESSAGE_IDS: Record<GetConnectCommandOutput200ErrorCode, string> = {
  dns_failed: "connectorForm.sections.connect.dnsFailed",
  connection_refused: "connectorForm.sections.connect.connectionRefused",
  timeout: "connectorForm.sections.connect.timeout",
  unknown: "connectorForm.sections.connect.unknown",
  none: "connectorForm.sections.connect.unknown",
};

export const useCheckSection = (formType: "source" | "destination", connectorDefinition: ConnectorDefinition) => {
  const { formatMessage } = useIntl();
  // A separate check for "Continue" between form sections, so its result never shows as the outcome of "Save & Test"
  const { testConnector: testSection } = useTestConnectorCommand({ formType });
  const testConnectTarget = useConnectCommand();

  const checkSectionWithConnector = async (values: ConnectorFormValues) => {
    const output = await testSection({ ...values, serviceType: Connector.id(connectorDefinition) });
    // A failed check throws with its job info, but a check that was aborted resolves as failed
    if (output.status !== "succeeded") {
      throw new Error(formatMessage({ id: "connector.check.failed" }));
    }
  };

  return async (values: ConnectorFormValues, connectTarget?: ConnectTarget) => {
    if (!connectTarget) {
      return checkSectionWithConnector(values);
    }

    const output = await testConnectTarget(connectTarget);
    // No output means that the server refused to run the command, for example because of its rate limit. A failure
    // reason means that the command failed on the platform side. Neither result says that the host is wrong.
    if (!output || output.failure_reason) {
      return checkSectionWithConnector(values);
    }

    if (!output.reachable) {
      const { host, port } = connectTarget;
      throw new Error(formatMessage({ id: CONNECT_FAILURE_MESSAGE_IDS[output.error_code] }, { host, port }));
    }
  };
};
