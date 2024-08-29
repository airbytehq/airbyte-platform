import { useFloating, offset, flip, autoUpdate } from "@floating-ui/react-dom";
import { Popover, PopoverButton, PopoverPanel } from "@headlessui/react";
import { useIntl } from "react-intl";

import { Button, ButtonProps } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Icon } from "components/ui/Icon";
import { Switch } from "components/ui/Switch";
import { Text } from "components/ui/Text";

import { useExperiment } from "hooks/services/Experiment";
import { useConnectorBuilderFormState } from "services/connectorBuilder/ConnectorBuilderStateService";

import styles from "./AssistConfigButton.module.scss";
import { BuilderField } from "../BuilderField";

type ChangeEventFunction = (e: React.ChangeEvent<HTMLInputElement>) => void;

export const AssistForm: React.FC = () => {
  const { formatMessage } = useIntl();

  return (
    <FlexContainer direction="column" gap="lg" className={styles.assistForm}>
      <BuilderField
        type="string"
        optional
        label={formatMessage({ id: "connectorBuilder.globalConfiguration.assist.docsUrl" })}
        placeholder={formatMessage({ id: "connectorBuilder.globalConfiguration.assist.docsUrl.placeholder" })}
        manifestPath="metadata.assist.docsUrl"
        path="formValues.assist.docsUrl"
      />
      <BuilderField
        type="string"
        optional
        label={formatMessage({ id: "connectorBuilder.globalConfiguration.assist.openApiSpecUrl" })}
        placeholder={formatMessage({ id: "connectorBuilder.globalConfiguration.assist.openApiSpecUrl.placeholder" })}
        manifestPath="metadata.assist.openApiSpecUrl"
        path="formValues.assist.openApiSpecUrl"
      />
    </FlexContainer>
  );
};

const AssistSwitch: React.FC = () => {
  const { assistEnabled, setAssistEnabled } = useConnectorBuilderFormState();
  const onChange: ChangeEventFunction = (e) => {
    setAssistEnabled(e.currentTarget.checked);
  };

  return <Switch checked={assistEnabled} onChange={onChange} />;
};

const AssistTitle = () => {
  const { formatMessage } = useIntl();
  const { assistEnabled } = useConnectorBuilderFormState();

  return (
    <FlexContainer direction="row" alignItems="center" gap="sm">
      <Icon type="aiStars" color={assistEnabled ? "highlight" : "disabled"} size="md" />
      <Heading as="h3" size="sm" className={styles.assistTitle}>
        {formatMessage({ id: "connectorBuilder.assist.configModal.title" })}
      </Heading>
    </FlexContainer>
  );
};

const AssistConfigPanel = () => {
  const { formatMessage } = useIntl();

  return (
    <FlexContainer direction="column" gap="lg" className={styles.assistConfigPanel}>
      <FlexContainer direction="row" justifyContent="space-between" alignItems="center">
        <AssistTitle />
        <AssistSwitch />
      </FlexContainer>
      <Text as="span" color="grey400" size="sm">
        {formatMessage({ id: "connectorBuilder.assist.configModal.description" })}
      </Text>
      <AssistForm />
    </FlexContainer>
  );
};

const AIButton = (props: ButtonProps) => {
  const { formatMessage } = useIntl();
  const { assistEnabled } = useConnectorBuilderFormState();

  const variant = assistEnabled ? "highlight" : "secondary";

  return (
    <Button variant={variant} icon="aiStars" {...props} type="button">
      {formatMessage({ id: "connectorBuilder.assist.configModal.button" })}
    </Button>
  );
};

const AssistConfigButton = () => {
  const isAIEnabled = useExperiment("connectorBuilder.aiAssist.enabled", false);

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
