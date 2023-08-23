import { format, parseISO } from "date-fns";
import { PropsWithChildren, ReactNode } from "react";
import { FormattedMessage } from "react-intl";
import { useNavigate } from "react-router-dom";

import { Button } from "components/ui/Button";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Message } from "components/ui/Message";
import { Text } from "components/ui/Text";
import { TextWithHTML } from "components/ui/TextWithHTML";

import { useUpgradeConnectorVersion } from "core/api";
import { ActorDefinitionVersionBreakingChanges, SupportState } from "core/request/AirbyteClient";
import { FeatureItem, useFeature } from "core/services/features";
import { useConfirmationModalService } from "hooks/services/ConfirmationModal";
import { useNotificationService } from "hooks/services/Notification";
import { useConnectionList } from "hooks/services/useConnectionHook";
import { SourcePaths } from "pages/routePaths";

import styles from "./BreakingChangeBanner.module.scss";

const MAX_CONNECTION_NAMES = 5;

interface BreakingChangeBannerProps {
  breakingChanges: ActorDefinitionVersionBreakingChanges;
  supportState: SupportState;
  connectorId: string;
  connectorName: string;
  connectorType: "source" | "destination";
  connectorDefinitionId: string;
}

export const BreakingChangeBanner = ({
  breakingChanges,
  supportState,
  connectorId,
  connectorName,
  connectorType,
  connectorDefinitionId,
}: BreakingChangeBannerProps) => {
  const navigate = useNavigate();
  const { openConfirmationModal, closeConfirmationModal } = useConfirmationModalService();
  const { connections } = useConnectionList({
    [connectorType === "source" ? "sourceId" : "destinationId"]: [connectorId],
  });
  const { mutateAsync: upgradeVersion } = useUpgradeConnectorVersion(connectorType, connectorId, connectorDefinitionId);
  const { registerNotification } = useNotificationService();
  const connectorBreakingChangeDeadlines = useFeature(FeatureItem.ConnectorBreakingChangeDeadlines);

  const formattedUpgradeDeadline = format(parseISO(breakingChanges.minUpgradeDeadline), "MMMM d, yyyy");
  // there should always be at least one breaking change, or else this banner wouldn't be shown
  const migrationGuideUrl = breakingChanges.upcomingBreakingChanges[0].migrationDocumentationUrl;

  const handleSubmitUpgrade = () => {
    upgradeVersion(connectorId)
      .then(() => {
        registerNotification({
          type: "success",
          id: `connector.upgradeSuccess.${connectorId}`,
          text: (
            <FormattedMessage
              id="connector.breakingChange.upgradeToast.success"
              values={{
                type: connectorType === "source" ? "Source" : "Destination",
                a: (node: ReactNode) => <Anchor href={migrationGuideUrl}>{node}</Anchor>,
              }}
            />
          ),
          timeout: false,
        });
      })
      .catch(() => {
        registerNotification({
          type: "error",
          id: `connector.upgradeError.${connectorId}`,
          text: (
            <FormattedMessage id="connector.breakingChange.upgradeToast.failure" values={{ type: connectorType }} />
          ),
        });
      });
    closeConfirmationModal();
  };

  return (
    <Message
      type={connectorBreakingChangeDeadlines && supportState === "unsupported" ? "error" : "warning"}
      iconOverride="warning"
      onAction={() => {
        openConfirmationModal({
          title: `connector.breakingChange.upgradeModal.title.${connectorType}`,
          text:
            connections.length === 0
              ? "connector.breakingChange.upgradeModal.areYouSure"
              : "connector.breakingChange.upgradeModal.text",
          textValues: { name: connectorName, count: connections.length, type: connectorType },
          submitButtonText: "connector.breakingChange.upgradeModal.confirm",
          submitButtonVariant: "primary",
          additionalContent:
            connections.length > 0 ? (
              <FlexContainer direction="column" className={styles.additionalContent}>
                <FlexItem className={styles.connectionsContainer}>
                  <ul className={styles.connectionsList}>
                    {connections.slice(0, MAX_CONNECTION_NAMES).map((connection, idx) => (
                      <li key={idx}>
                        <Text bold size="lg">
                          {connection.name}
                        </Text>
                      </li>
                    ))}
                  </ul>
                  {connections.length > MAX_CONNECTION_NAMES && (
                    <Button
                      type="button"
                      variant="link"
                      className={styles.moreConnections}
                      onClick={() => {
                        closeConfirmationModal();
                        navigate(SourcePaths.Connections);
                      }}
                    >
                      <Text size="lg" italicized>
                        <FormattedMessage
                          id="connector.breakingChange.upgradeModal.moreConnections"
                          values={{ count: connections.length - MAX_CONNECTION_NAMES }}
                        />
                      </Text>
                    </Button>
                  )}
                </FlexItem>
                <FormattedMessage
                  id="connector.breakingChange.upgradeModal.areYouSure"
                  values={{ type: connectorType }}
                />
              </FlexContainer>
            ) : undefined,
          onSubmit: handleSubmitUpgrade,
        });
      }}
      actionBtnText={<FormattedMessage id="connector.breakingChange.upgradeButton" />}
      actionBtnProps={{ className: styles.upgradeButton }}
      text={
        <FlexContainer direction="column" gap="lg">
          <FlexContainer direction="column" gap="sm">
            <Text size="lg" bold>
              <FormattedMessage id="connector.breakingChange.title" />
            </Text>
            <Text>
              <FormattedMessage id="connector.breakingChange.pendingUpgrade" values={{ name: connectorName }} />
            </Text>
          </FlexContainer>
          {breakingChanges.upcomingBreakingChanges.map((breakingChange) => (
            <FlexContainer direction="column" gap="xs" key={breakingChange.version}>
              <Text bold>
                <FormattedMessage id="connector.breakingChange.version" values={{ version: breakingChange.version }} />
              </Text>
              <Text>
                <TextWithHTML text={breakingChange.message} />
              </Text>
            </FlexContainer>
          ))}
          <Text>
            {connectorBreakingChangeDeadlines && (
              <>
                <FormattedMessage
                  id={
                    supportState === "unsupported"
                      ? "connector.breakingChange.unsupportedUpgrade"
                      : "connector.breakingChange.deprecatedUpgrade"
                  }
                  values={{
                    name: connectorName,
                    date: formattedUpgradeDeadline,
                    type: connectorType,
                  }}
                />{" "}
              </>
            )}
            <span>
              <FormattedMessage
                id="connector.breakingChange.moreInfo"
                values={{ a: (node: ReactNode) => <Anchor href={migrationGuideUrl}>{node}</Anchor> }}
              />
            </span>
          </Text>
        </FlexContainer>
      }
    />
  );
};

const Anchor: React.FC<PropsWithChildren<{ href: string }>> = ({ href, children }) => (
  <a href={href} target="_blank" rel="noreferrer">
    {children}
  </a>
);
