import classNames from "classnames";
import { FormattedMessage } from "react-intl";

import { convertToConnectorDefinitionWithMetrics, MetricIcon } from "components/connector/ConnectorQualityMetrics";
import { ConnectorIcon } from "components/ConnectorIcon";
import { FlexContainer } from "components/ui/Flex";
import { Icon } from "components/ui/Icon";
import { Link } from "components/ui/Link";
import { SupportLevelBadge } from "components/ui/SupportLevelBadge";
import { Text } from "components/ui/Text";

import { useCurrentWorkspaceLink } from "area/workspace/utils";
import { ConnectorDefinitionOrEnterpriseStub } from "core/domain/connector";
import { RoutePaths } from "pages/routePaths";

import styles from "./ConnectorButton.module.scss";

interface ConnectorButtonProps<T extends ConnectorDefinitionOrEnterpriseStub> {
  className?: string;
  onClick: (definition: T) => void;
  definition: T;
  showMetrics?: boolean;
  maxLines: 2 | 3;
}

const EnterpriseBadge = () => {
  return (
    <span>
      <SupportLevelBadge supportLevel="enterprise" custom={false} />
    </span>
  );
};

export const ConnectorButton = <T extends ConnectorDefinitionOrEnterpriseStub>({
  className,
  definition,
  onClick,
  showMetrics,
  maxLines,
}: ConnectorButtonProps<T>) => {
  return (
    <button className={classNames(styles.button, className)} onClick={() => onClick(definition)}>
      <FlexContainer alignItems="center" className={styles.iconAndName}>
        <ConnectorIcon icon={definition.icon} className={styles.icon} />
        <Text
          size="sm"
          className={classNames(styles.text, {
            [styles.twoMaxLines]: maxLines === 2,
            [styles.threeMaxLines]: maxLines === 3,
          })}
        >
          {definition.name}
        </Text>
      </FlexContainer>
      {
        // Conditionally render the metrics only if it is not an EnterpriseSourceStub
        showMetrics && !("isEnterprise" in definition) && (
          <FlexContainer className={styles.metrics}>
            <MetricIcon metric="success" connectorDefinition={convertToConnectorDefinitionWithMetrics(definition)} />
            <MetricIcon metric="usage" connectorDefinition={convertToConnectorDefinitionWithMetrics(definition)} />
          </FlexContainer>
        )
      }
      {(("isEnterprise" in definition && definition.isEnterprise === true) ||
        ("enterprise" in definition && definition.enterprise === true)) && <EnterpriseBadge />}
    </button>
  );
};

interface BuilderConnectorButtonProps {
  className?: string;
  layout: "horizontal" | "vertical";
}
export const BuilderConnectorButton: React.FC<BuilderConnectorButtonProps> = ({ className, layout }) => {
  const createLink = useCurrentWorkspaceLink();

  return (
    <Link
      to={createLink(`/${RoutePaths.ConnectorBuilder}`)}
      className={classNames(styles.button, className)}
      variant="primary"
    >
      <FlexContainer alignItems="center" className={styles.iconAndName}>
        <Icon type="wrench" color="action" size="xl" className={styles.builderPromptIcon} />
        <FlexContainer
          direction={layout === "horizontal" ? "row" : "column"}
          alignItems={layout === "horizontal" ? "center" : "flex-start"}
          gap={layout === "horizontal" ? "md" : "sm"}
          className={styles.builderPrompt}
        >
          <Text size="lg" className={classNames(styles.builderPromptText, styles.builderPromptPrimary)}>
            <FormattedMessage id="connectorBuilder.builderPrompt.primary" />
          </Text>
          <Text className={classNames(styles.builderPromptText, styles.builderPromptSecondary)} color="grey400">
            <FormattedMessage id="connectorBuilder.builderPrompt.secondary" />
          </Text>
        </FlexContainer>
      </FlexContainer>
    </Link>
  );
};
