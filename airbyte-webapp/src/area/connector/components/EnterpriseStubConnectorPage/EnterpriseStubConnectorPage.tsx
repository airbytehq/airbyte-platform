import classNames from "classnames";
import { FormattedMessage, useIntl } from "react-intl";
import { useParams } from "react-router-dom";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex/FlexContainer";
import { Icon } from "components/ui/Icon";
import { ExternalLink } from "components/ui/Link";
import { PageHeaderWithNavigation } from "components/ui/PageHeader/PageHeaderWithNavigation";
import { Text } from "components/ui/Text";

import { useListEnterpriseSourceStubs, useListEnterpriseDestinationStubs } from "core/api";
import { EnterpriseConnectorStubType } from "core/domain/connector";
import { links } from "core/utils/links";
import { useAirbyteTheme } from "core/utils/useAirbyteTheme";
import { RoutePaths } from "pages/routePaths";

import blurred_source_form from "./blurred_source_form.webp";
import { EnterpriseDocumentationPanel } from "./EnterpriseDocumentationPanel";
import styles from "./EnterpriseStubConnectorPage.module.scss";

const appendConnectorTracking = (name: string) => {
  const lowerCaseName: string = name.toLowerCase();
  return `${links.contactSales}?utm_source=airbyte&utm_medium=product&utm_content=enterprise_connector_${lowerCaseName}`;
};

interface EnterpriseStubConnectorPageProps {
  connectorType: "source" | "destination";
}

export const EnterpriseStubConnectorPage: React.FC<EnterpriseStubConnectorPageProps> = ({ connectorType }) => {
  const { theme } = useAirbyteTheme();
  const params = useParams<{ workspaceId: string; id: string }>();
  const { enterpriseSourceDefinitionsMap = new Map() } =
    useListEnterpriseSourceStubs({
      enabled: connectorType === "source",
    }) || {};
  const { enterpriseDestinationDefinitionsMap = new Map() } =
    useListEnterpriseDestinationStubs({
      enabled: connectorType === "destination",
    }) || {};

  const enterpriseConnector = params.id
    ? connectorType === "source"
      ? (enterpriseSourceDefinitionsMap.get(params.id) as EnterpriseConnectorStubType)
      : (enterpriseDestinationDefinitionsMap.get(params.id) as EnterpriseConnectorStubType)
    : undefined;

  const breadcrumbBasePath = `/${RoutePaths.Workspaces}/${params.workspaceId}/${
    connectorType === "source" ? RoutePaths.Source : RoutePaths.Destination
  }`;

  const { formatMessage } = useIntl();

  const onGoBack = () => {
    window.history.back();
  };

  const breadcrumbsData = [
    {
      label: formatMessage({ id: connectorType === "source" ? "sidebar.sources" : "sidebar.destinations" }),
      to: `${breadcrumbBasePath}/`,
    },
    { label: formatMessage({ id: connectorType === "source" ? "sources.newSource" : "destinations.newDestination" }) },
  ];

  if (!enterpriseConnector) {
    return null;
  }

  return (
    <div className={styles.pageContent}>
      <div className={styles.leftSection}>
        <PageHeaderWithNavigation breadcrumbsData={breadcrumbsData} />
        <div className={styles.backButtonContainer}>
          <Button variant="clear" onClick={onGoBack} icon="chevronLeft" iconSize="lg">
            <FormattedMessage id="connectorBuilder.backButtonLabel" />
          </Button>
        </div>
        <FlexContainer className={styles.blurredForm}>
          <img
            src={blurred_source_form}
            alt=""
            className={classNames(styles.backgroundImage, {
              [styles.backgroundImageDark]: theme === "airbyteThemeDark",
            })}
          />
          <FlexContainer className={styles.content}>
            <Text size="lg">
              <FormattedMessage id="connector.enterprise.interest" />
            </Text>
            <ExternalLink
              variant="button"
              opensInNewTab
              href={appendConnectorTracking(enterpriseConnector.name)}
              className={styles.button}
            >
              <FormattedMessage id="credits.talkToSales" />
              <Icon type="share" />
            </ExternalLink>
          </FlexContainer>
        </FlexContainer>
      </div>

      <div className={styles.rightSection}>
        {enterpriseConnector && (
          <EnterpriseDocumentationPanel
            id={enterpriseConnector.id}
            name={enterpriseConnector.name}
            documentationUrl={enterpriseConnector.url}
          />
        )}
      </div>
    </div>
  );
};
