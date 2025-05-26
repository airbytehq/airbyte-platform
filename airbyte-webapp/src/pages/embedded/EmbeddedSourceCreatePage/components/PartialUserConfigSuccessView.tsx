import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Text } from "components/ui/Text";

import { SvgIcon } from "area/connector/utils";

import styles from "./PartialUserConfigSuccessView.module.scss";
import { useEmbeddedSourceParams } from "../hooks/useEmbeddedSourceParams";

interface PartialUserConfigSuccessViewProps {
  successType: "create" | "delete" | "update";
  connectorName: string;
  icon?: string;
}

export const PartialUserConfigSuccessView: React.FC<PartialUserConfigSuccessViewProps> = ({
  successType,
  connectorName,
  icon,
}) => {
  const { clearSelectedConfig, clearSelectedTemplate } = useEmbeddedSourceParams();

  return (
    <FlexContainer className={styles.content} direction="column" gap="lg" justifyContent="space-between">
      {successType !== "delete" && (
        <FlexContainer alignItems="center" gap="sm" justifyContent="center">
          <FlexContainer className={styles.iconContainer} aria-hidden="true" alignItems="center">
            <SvgIcon src={icon} />
          </FlexContainer>
          <p>{connectorName}</p>
        </FlexContainer>
      )}
      <FlexContainer
        direction="column"
        gap="lg"
        justifyContent="center"
        alignItems="center"
        className={styles.successContainer}
      >
        <Icon size="xl" type="checkCircle" color="primary" />
        <Text size="lg">
          <FormattedMessage id="partialUserConfig.success.title" />
        </Text>
        <Text size="md">
          {successType === "delete" ? (
            <FormattedMessage id="partialUserConfig.delete.success" values={{ connectorName }} />
          ) : (
            <FormattedMessage id="partialUserConfig.success.description" />
          )}
        </Text>
      </FlexContainer>
      <div className={styles.buttonContainer}>
        <Button
          full
          onClick={() => {
            clearSelectedConfig();
            clearSelectedTemplate();
          }}
        >
          <FormattedMessage id="partialUserConfig.backToIntegrations" />
        </Button>
      </div>
    </FlexContainer>
  );
};
