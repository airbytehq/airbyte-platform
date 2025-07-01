import { useFloating, offset, flip, autoUpdate } from "@floating-ui/react-dom";
import { Popover, PopoverButton, PopoverPanel } from "@headlessui/react";
import { useIntl, FormattedMessage } from "react-intl";

import { FormControl } from "components/forms/FormControl";
import { Badge } from "components/ui/Badge";
import { Button, ButtonProps } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Icon } from "components/ui/Icon";
import { ExternalLink } from "components/ui/Link";
import { Switch } from "components/ui/Switch";
import { Text } from "components/ui/Text";

import { Action, Namespace, useAnalyticsService } from "core/services/analytics";
import { useTrackMount } from "core/services/analytics/useAnalyticsService";
import { links } from "core/utils/links";
import { useExperiment } from "hooks/services/Experiment";
import { useConnectorBuilderFormState } from "services/connectorBuilder/ConnectorBuilderStateService";

import styles from "./AssistConfigButton.module.scss";

type ChangeEventFunction = (e: React.ChangeEvent<HTMLInputElement>) => void;

export const AssistForm: React.FC = () => {
  const { formatMessage } = useIntl();

  return (
    <FlexContainer direction="column" gap="lg" className={styles.assistForm}>
      <FormControl
        fieldType="input"
        name="manifest.metadata.assist.docsUrl"
        label={formatMessage({ id: "connectorBuilder.assist.config.docsUrl.label" })}
        placeholder={formatMessage({ id: "connectorBuilder.assist.config.docsUrl.placeholder" })}
        labelTooltip={formatMessage({ id: "connectorBuilder.assist.config.docsUrl.tooltip" })}
        reserveSpaceForError={false}
      />
      <FormControl
        fieldType="input"
        name="manifest.metadata.assist.openapiSpecUrl"
        label={formatMessage({ id: "connectorBuilder.assist.config.openapiSpecUrl.label" })}
        placeholder={formatMessage({ id: "connectorBuilder.assist.config.openapiSpecUrl.placeholder" })}
        labelTooltip={formatMessage({ id: "connectorBuilder.assist.config.openapiSpecUrl.tooltip" })}
        reserveSpaceForError={false}
      />
    </FlexContainer>
  );
};

const AssistSwitch: React.FC = () => {
  const { assistEnabled, setAssistEnabled, projectId } = useConnectorBuilderFormState();
  const analyticsService = useAnalyticsService();

  const onChange: ChangeEventFunction = (e) => {
    setAssistEnabled(e.currentTarget.checked);
    const actionToTrack = e.currentTarget.checked
      ? Action.CONNECTOR_BUILDER_ASSIST_ENABLED
      : Action.CONNECTOR_BUILDER_ASSIST_DISABLED;
    analyticsService.track(Namespace.CONNECTOR_BUILDER, actionToTrack, {
      projectId,
    });
  };

  return <Switch checked={assistEnabled} onChange={onChange} />;
};

const AssistTitle = () => {
  const { assistEnabled } = useConnectorBuilderFormState();

  return (
    <FlexContainer direction="row" alignItems="center" gap="sm">
      <Icon type="aiStars" color={assistEnabled ? "magic" : "disabled"} size="md" />
      <Heading as="h3" size="sm" className={styles.assistTitle}>
        <FormattedMessage id="connectorBuilder.assist.config.title" />
      </Heading>
      <Badge variant="blue">
        <FormattedMessage id="ui.badge.beta" />
      </Badge>
    </FlexContainer>
  );
};

const AssistConfigPanel = () => {
  const { projectId } = useConnectorBuilderFormState();

  useTrackMount({
    namespace: Namespace.CONNECTOR_BUILDER,
    mountAction: Action.CONNECTOR_BUILDER_ASSIST_CONFIG_OPENED,
    unmountAction: Action.CONNECTOR_BUILDER_ASSIST_CONFIG_CLOSED,
    params: {
      projectId,
    },
  });

  return (
    <FlexContainer direction="column" gap="lg" className={styles.assistConfigPanel}>
      <FlexContainer direction="row" justifyContent="space-between" alignItems="center">
        <AssistTitle />
        <AssistSwitch />
      </FlexContainer>
      <Text as="span" color="grey400" size="sm">
        <FormattedMessage
          id="connectorBuilder.assist.config.description"
          values={{
            lnk: (children: React.ReactNode) => (
              <ExternalLink href={links.connectorBuilderAssist}>{children}</ExternalLink>
            ),
          }}
        />
      </Text>
      <AssistForm />
    </FlexContainer>
  );
};

const AIButton = (props: ButtonProps) => {
  const { assistEnabled } = useConnectorBuilderFormState();

  const variant = assistEnabled ? "magic" : "secondary";

  return (
    <Button variant={variant} icon="aiStars" {...props} type="button">
      <FormattedMessage id="connectorBuilder.assist.config.button" />
    </Button>
  );
};

const AssistConfigButton = () => {
  const isAIEnabled = useExperiment("connectorBuilder.aiAssist.enabled");

  const { x, y, reference, floating, strategy } = useFloating({
    middleware: [offset(5), flip()],
    whileElementsMounted: autoUpdate,
    placement: "bottom",
  });

  if (!isAIEnabled) {
    return null;
  }

  return (
    <Popover>
      {({ open }) => (
        <>
          <PopoverButton ref={reference} as="div">
            <AIButton />
          </PopoverButton>
          {open && (
            <PopoverPanel
              ref={floating}
              className={styles.popoverPanel}
              style={{
                position: strategy,
                top: y ?? 0,
                left: x ?? 0,
              }}
            >
              <AssistConfigPanel />
            </PopoverPanel>
          )}
        </>
      )}
    </Popover>
  );
};

export default AssistConfigButton;
