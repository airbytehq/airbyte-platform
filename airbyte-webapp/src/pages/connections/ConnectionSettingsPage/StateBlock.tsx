import React, { useCallback, useMemo, useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { CodeEditor } from "components/ui/CodeEditor";
import { CopyButton } from "components/ui/CopyButton";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Message } from "components/ui/Message";
import { Text } from "components/ui/Text";

import { useCreateOrUpdateState, useGetConnectionState } from "core/api";
import { AirbyteCatalog, ConnectionState, StreamState } from "core/api/types/AirbyteClient";
import { haveSameShape } from "core/utils/objects";
import { useConfirmationModalService } from "hooks/services/ConfirmationModal";

import styles from "./StateBlock.module.scss";

interface StateBlockProps {
  connectionId: string;
  syncCatalog: AirbyteCatalog;
}

function convertStateToString(state: ConnectionState): string {
  const format = (value: unknown) => JSON.stringify(value, null, 2);
  switch (state.stateType) {
    case "global":
      return format(state.globalState);
    case "stream":
      return format(state.streamState);
    case "legacy":
      return format(state.state);
    case "not_set":
      return "";
  }
}

export const StateBlock: React.FC<StateBlockProps> = ({ connectionId, syncCatalog }) => {
  const { formatMessage } = useIntl();
  const existingState = useGetConnectionState(connectionId);
  const { mutateAsync: updateState, isLoading } = useCreateOrUpdateState();
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();

  const hasIncrementalStream = useMemo(() => {
    return syncCatalog.streams.some((stream) => stream.config?.syncMode === "incremental");
  }, [syncCatalog.streams]);

  const existingStateString = useMemo(() => convertStateToString(existingState), [existingState]);

  const [stateDraft, setStateDraft] = useState<string>(existingStateString);

  const { newState, errorMessage } = useMemo((): {
    newState?: ConnectionState;
    errorMessage?: string;
  } => {
    let stateDraftJson;
    try {
      stateDraftJson = JSON.parse(stateDraft);
    } catch (e) {
      return { errorMessage: formatMessage({ id: "connection.state.error.invalidJson" }) };
    }

    if (Array.isArray(stateDraftJson)) {
      if (isStreamStateArray(stateDraftJson)) {
        return { newState: { connectionId, stateType: "stream", streamState: stateDraftJson } };
      }

      return { errorMessage: formatMessage({ id: "connection.state.error.invalidStreamStateArray" }) };
    }

    if (typeof stateDraftJson === "object" && stateDraftJson !== null && "streamStates" in stateDraftJson) {
      if (!Array.isArray(stateDraftJson.streamStates)) {
        return { errorMessage: formatMessage({ id: "connection.state.error.invalidStreamStates" }) };
      }

      if (isStreamStateArray(stateDraftJson.streamStates)) {
        return { newState: { connectionId, stateType: "global", globalState: stateDraftJson } };
      }

      return { errorMessage: formatMessage({ id: "connection.state.error.invalidStreamStateArray" }) };
    }

    return { newState: { connectionId, stateType: "legacy", state: stateDraftJson } };
  }, [connectionId, formatMessage, stateDraft]);

  const handleStateUpdate = useCallback(() => {
    if (newState === undefined) {
      return;
    }
    const draftHasSameShape = haveSameShape(existingState, newState);
    openConfirmationModal({
      title: "connection.state.confirm.title",
      text:
        draftHasSameShape || existingState.stateType === "not_set"
          ? "connection.state.confirm.message"
          : "connection.state.confirm.shapeChanged",
      submitButtonText: "connection.state.update",
      onSubmit: () => {
        updateState({
          connectionId,
          connectionState: newState,
        }).then((updatedState: ConnectionState) => setStateDraft(convertStateToString(updatedState)));
        closeConfirmationModal();
      },
    });
  }, [closeConfirmationModal, connectionId, existingState, newState, openConfirmationModal, updateState]);
  const title = (
    <Heading as="h5" size="sm" className={styles.title}>
      <FormattedMessage id="connection.state.title" />
    </Heading>
  );

  return (
    <Card withPadding>
      <FlexContainer direction="column">
        {!hasIncrementalStream ? (
          <>
            {title}
            <Text>
              <FormattedMessage id="connection.state.noIncremental" />
            </Text>
          </>
        ) : (
          <>
            <FlexContainer alignItems="center" justifyContent="space-between">
              {title}
              <CopyButton
                content={stateDraft ?? existingStateString}
                title={formatMessage({ id: "connection.state.copyTitle" })}
              />
            </FlexContainer>

            <CodeEditor
              value={stateDraft ?? existingStateString}
              height={styles.stateEditorHeight}
              language="json"
              automaticLayout
              showSuggestions={false}
              onChange={(value) => {
                setStateDraft(value ?? "");
              }}
            />

            <FlexContainer direction="column">
              {errorMessage ? (
                <Message type="error" text={errorMessage} />
              ) : (
                <Message
                  type="warning"
                  text={<FormattedMessage id="connection.state.warning" />}
                  secondaryText={
                    <FormattedMessage id="connection.state.warning.secondary" values={{ br: () => <br /> }} />
                  }
                />
              )}
              <FlexContainer alignItems="center" justifyContent="flex-end">
                <Button
                  variant="secondary"
                  onClick={() => setStateDraft(existingStateString)}
                  disabled={stateDraft === existingStateString}
                >
                  <FormattedMessage id="connection.state.revert" />
                </Button>
                <Button
                  onClick={handleStateUpdate}
                  isLoading={isLoading}
                  disabled={stateDraft === existingStateString || newState === undefined || errorMessage !== undefined}
                >
                  <FormattedMessage id="connection.state.update" />
                </Button>
              </FlexContainer>
            </FlexContainer>
          </>
        )}
      </FlexContainer>
    </Card>
  );
};

const isStreamStateArray = (array: unknown[]): array is StreamState[] => {
  return array.every(
    (item) =>
      typeof item === "object" &&
      item !== null &&
      "streamDescriptor" in item &&
      typeof item.streamDescriptor === "object" &&
      item.streamDescriptor !== null &&
      "name" in item.streamDescriptor &&
      typeof item.streamDescriptor.name === "string"
  );
};
