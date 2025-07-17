import { PropsWithChildren, ReactNode, useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Input } from "components/ui/Input";
import { Markdown } from "components/ui/Markdown";
import { Message } from "components/ui/Message";
import { Modal, ModalBody, ModalFooter } from "components/ui/Modal";
import { Text } from "components/ui/Text";

import { useUpgradeConnectorVersion } from "core/api";
import { ActorDefinitionVersionRead, DeadlineAction } from "core/api/types/AirbyteClient";
import { getHumanReadableUpgradeDeadline } from "core/domain/connector";
import { FeatureItem, useFeature } from "core/services/features";
import { useNotificationService } from "hooks/services/Notification";

import styles from "./BreakingChangeBanner.module.scss";

interface BreakingChangeBannerProps {
  actorDefinitionVersion: ActorDefinitionVersionRead;
  connectorId: string;
  connectorName: string;
  connectorType: "source" | "destination";
  connectorDefinitionId: string;
}

export const BreakingChangeBanner = ({
  actorDefinitionVersion,
  connectorId,
  connectorName,
  connectorType,
  connectorDefinitionId,
}: BreakingChangeBannerProps) => {
  const [isConfirmUpdateOpen, setConfirmUpdateOpen] = useState(false);

  const connectorBreakingChangeDeadlines = useFeature(FeatureItem.ConnectorBreakingChangeDeadlines);

  const supportState = actorDefinitionVersion.supportState;

  // Do not render if no breaking changes are found.
  const breakingChanges = actorDefinitionVersion?.breakingChanges;
  if (!breakingChanges) {
    return null;
  }

  const formattedUpgradeDeadline = getHumanReadableUpgradeDeadline(actorDefinitionVersion);

  // there should always be at least one breaking change, or else this banner wouldn't be shown
  const migrationGuideUrl = breakingChanges.upcomingBreakingChanges[0].migrationDocumentationUrl;

  const messageType = connectorBreakingChangeDeadlines && supportState === "unsupported" ? "error" : "warning";

  const isAutoUpgrade = actorDefinitionVersion.breakingChanges?.deadlineAction === DeadlineAction.auto_upgrade;

  const unSupportedMessageId = isAutoUpgrade
    ? "connector.breakingChange.autoupgrade.unsupportedUpgrade"
    : "connector.breakingChange.unsupportedUpgrade";

  const deprecatedMessageId = isAutoUpgrade
    ? "connector.breakingChange.autoupgrade.deprecatedUpgrade"
    : "connector.breakingChange.deprecatedUpgrade";

  const breakingChangeMessageId = supportState === "unsupported" ? unSupportedMessageId : deprecatedMessageId;

  const title = isAutoUpgrade ? "connector.breakingChange.autoupgrade.title" : "connector.breakingChange.title";
  return (
    <Message
      type={messageType}
      data-testid={`breaking-change-${messageType}-actor-banner`}
      iconOverride="warning"
      onAction={() => setConfirmUpdateOpen(true)}
      actionBtnText={<FormattedMessage id="connector.breakingChange.upgradeButton" />}
      text={
        <FlexContainer direction="column" gap="lg">
          <FlexContainer direction="column" gap="sm">
            <Text size="lg" bold>
              <FormattedMessage id={title} />
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
              <Markdown className={styles.breakingChangeMessage} content={breakingChange.message} />
            </FlexContainer>
          ))}
          <Text>
            {connectorBreakingChangeDeadlines && (
              <>
                <FormattedMessage
                  id={breakingChangeMessageId}
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
    >
      {isConfirmUpdateOpen && (
        <ConfirmUpdateModal
          connectorId={connectorId}
          connectorDefinitionId={connectorDefinitionId}
          connectorName={connectorName}
          connectorType={connectorType}
          migrationGuideUrl={migrationGuideUrl}
          onClose={() => setConfirmUpdateOpen(false)}
        />
      )}
    </Message>
  );
};

const Anchor: React.FC<PropsWithChildren<{ href: string }>> = ({ href, children }) => (
  <a href={href} target="_blank" rel="noreferrer">
    {children}
  </a>
);

interface ConfirmUpdateModalProps {
  connectorId: string;
  connectorDefinitionId: string;
  connectorName: string;
  connectorType: "source" | "destination";
  migrationGuideUrl: string;
  onClose: () => void;
}

const ConfirmUpdateModal = ({
  connectorId,
  connectorDefinitionId,
  connectorType,
  migrationGuideUrl,
  onClose,
}: ConfirmUpdateModalProps) => {
  const { formatMessage } = useIntl();
  const { mutateAsync: upgradeVersion, isLoading } = useUpgradeConnectorVersion(
    connectorType,
    connectorId,
    connectorDefinitionId
  );
  const { registerNotification } = useNotificationService();
  const allowUpdateConnectors = useFeature(FeatureItem.AllowUpdateConnectors);

  const [upgradeInputValue, setUpgradeInputValue] = useState("");

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
      })
      .finally(() => {
        onClose();
      });
  };

  return (
    <Modal
      title={<FormattedMessage id="connector.breakingChange.upgradeModal.title" values={{ type: connectorType }} />}
      onCancel={onClose}
      size="md"
    >
      <ModalBody>
        <FlexContainer direction="column">
          <Text>
            <FormattedMessage
              id="connector.breakingChange.upgradeModal.text"
              values={{
                type: connectorType,
              }}
            />
          </Text>
          <Message
            type="warning"
            text={
              <FlexContainer direction="column" gap="lg">
                <FlexItem>
                  <FormattedMessage
                    id="connector.breakingChange.upgradeModal.warning"
                    values={{
                      a: (node: ReactNode) => <Anchor href={migrationGuideUrl}>{node}</Anchor>,
                      br: () => <br />,
                    }}
                  />
                </FlexItem>
                {!allowUpdateConnectors && <FormattedMessage id="connector.breakingChange.upgradeModal.irreversible" />}
                <FlexItem>
                  <FormattedMessage id="connector.breakingChange.upgradeModal.typeUpgrade" />
                </FlexItem>
                <Input
                  containerClassName={styles.upgradeInput}
                  value={upgradeInputValue}
                  onChange={(event) => setUpgradeInputValue(event.target.value)}
                />
              </FlexContainer>
            }
          />
        </FlexContainer>
      </ModalBody>
      <ModalFooter>
        <Button type="button" variant="secondary" onClick={onClose}>
          <FormattedMessage id="form.cancel" />
        </Button>
        <Button
          type="button"
          onClick={handleSubmitUpgrade}
          isLoading={isLoading}
          disabled={
            upgradeInputValue !== formatMessage({ id: "connector.breakingChange.upgradeModal.typeUpgrade.value" })
          }
        >
          <FormattedMessage id="connector.breakingChange.upgradeModal.confirm" />
        </Button>
      </ModalFooter>
    </Modal>
  );
};
