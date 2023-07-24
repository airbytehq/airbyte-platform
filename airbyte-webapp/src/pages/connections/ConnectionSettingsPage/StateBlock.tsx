import React, { useMemo } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Card } from "components/ui/Card";
import { Heading } from "components/ui/Heading";

import { ConnectionState } from "core/request/AirbyteClient";
import { useGetConnectionState } from "hooks/services/useConnectionHook";

function formatState(
  legacyState: ConnectionState["state"],
  globalState: ConnectionState["globalState"],
  streamState: ConnectionState["streamState"],
  formatMessage: ReturnType<typeof useIntl>["formatMessage"]
) {
  // This hierarchy assumes that for those connections which have both global and per-stream state, the global state contains a meaningful copy of any state which would also be saved per-stream
  const formattedState = legacyState ?? globalState ?? streamState ?? null;
  return formattedState
    ? JSON.stringify(formattedState, null, 2)
    : formatMessage({ id: "tables.connectionState.noConnectionState" });
}

interface StateBlockProps {
  connectionId: string;
}

export const StateBlock: React.FC<StateBlockProps> = ({ connectionId }) => {
  const { formatMessage } = useIntl();
  const state = useGetConnectionState(connectionId);

  const stateString = useMemo(
    () => formatState(state.state, state.globalState, state.streamState, formatMessage),
    [formatMessage, state.state, state.globalState, state.streamState]
  );

  return (
    <Card withPadding>
      <Heading as="h5" size="sm">
        <FormattedMessage id="tables.connectionState.title" />
      </Heading>
      <pre style={{ maxHeight: 200, overflowY: "scroll" }}>{stateString}</pre>
    </Card>
  );
};
