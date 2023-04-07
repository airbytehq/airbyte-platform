import { faUser } from "@fortawesome/free-solid-svg-icons";
import { FontAwesomeIcon } from "@fortawesome/react-fontawesome";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer, FlexItem } from "components/ui/Flex";
import { Text } from "components/ui/Text";
import { InfoTooltip, Tooltip } from "components/ui/Tooltip";

import { useConnectorBuilderFormManagementState } from "services/connectorBuilder/ConnectorBuilderStateService";

import styles from "./BuilderInputPlaceholder.module.scss";

export interface BuilderFieldProps {
  label: string;
  tooltip?: string;
}

export const BuilderInputPlaceholder = (props: BuilderFieldProps) => {
  const { setTestInputOpen } = useConnectorBuilderFormManagementState();
  return (
    <FlexContainer alignItems="center">
      <FlexItem grow>
        <FlexContainer gap="none">
          <Text size="lg">{props.label}</Text>
          {props.tooltip && <InfoTooltip placement="top-start">{props.tooltip}</InfoTooltip>}
        </FlexContainer>
      </FlexItem>
      <Tooltip control={<FontAwesomeIcon icon={faUser} className={styles.tooltipTrigger} />}>
        <FormattedMessage id="connectorBuilder.placeholder.label" />
        <br />
        <Button
          variant="link"
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
