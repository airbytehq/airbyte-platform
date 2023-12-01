import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";
import { InfoTooltip, Tooltip } from "components/ui/Tooltip";

import { useConnectorBuilderFormManagementState } from "services/connectorBuilder/ConnectorBuilderStateService";

import styles from "./BuilderInputPlaceholder.module.scss";
import { getLabelAndTooltip } from "./manifestHelpers";

export interface BuilderFieldProps {
  label?: string;
  tooltip?: string;
  manifestPath?: string;
}

export const BuilderInputPlaceholder = (props: BuilderFieldProps) => {
  const { setTestInputOpen } = useConnectorBuilderFormManagementState();
  const { label, tooltip } = getLabelAndTooltip(props.label, props.tooltip, props.manifestPath, "", true, true);
  return (
    <FlexContainer alignItems="center">
      <FlexItem grow>
        <FlexContainer gap="none">
          <Text size="lg">{label}</Text>
          {tooltip && <InfoTooltip placement="top-start">{tooltip}</InfoTooltip>}
        </FlexContainer>
      </FlexItem>
      <Tooltip control={<Icon type="user" className={styles.tooltipTrigger} />}>
        <FormattedMessage id="connectorBuilder.placeholder.label" />
        <br />
        <Button
          variant="link"
          type="button"
          onClick={() => {
            setTestInputOpen(true);
          }}
        >
          <FormattedMessage id="connectorBuilder.placeholder.button" />
        </Button>
      </Tooltip>
    </FlexContainer>
  );
};
