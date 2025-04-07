import classNames from "classnames";
import { FormattedMessage, useIntl } from "react-intl";
import { useParams } from "react-router-dom";

import { EnterpriseDocumentationPanel } from "components/source/enterpriseStubs/EnterpriseDocumentationPanel";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex/FlexContainer";
import { Icon } from "components/ui/Icon";
import { ExternalLink } from "components/ui/Link";
import { PageHeaderWithNavigation } from "components/ui/PageHeader/PageHeaderWithNavigation";
import { Text } from "components/ui/Text";

import { useListEnterpriseStubsForWorkspace } from "core/api";
import { EnterpriseSourceStubType } from "core/domain/connector";
import { useAirbyteTheme } from "hooks/theme/useAirbyteTheme";
import { RoutePaths } from "pages/routePaths";

import blurred_source_form from "./blurred_source_form.webp";
import styles from "./EnterpriseSourcePage.module.scss";

// Set up URL with basic tracking parameters
const TALK_TO_SALES_URL: string = "https://airbyte.com/company/talk-to-sales?utm_source=airbyte&utm_medium=product";

const appendConnectorTracking = (name: string) => {
  const lowerCaseName: string = name.toLowerCase();
  return `${TALK_TO_SALES_URL}&utm_content=enterprise_connector_${lowerCaseName}`;
};

export const EnterpriseSourcePage: React.FC = () => {
  const { theme } = useAirbyteTheme();
  const params = useParams<{ workspaceId: string; id: string }>();
  const { enterpriseSourceDefinitionsMap } = useListEnterpriseStubsForWorkspace();

  const enterpriseSource = params.id
    ? (enterpriseSourceDefinitionsMap.get(params.id) as EnterpriseSourceStubType)
    : undefined;
  const breadcrumbBasePath = `/${RoutePaths.Workspaces}/${params.workspaceId}/${RoutePaths.Source}`;

  const { formatMessage } = useIntl();

  const onGoBack = () => {
    window.history.back();
  };

  const breadcrumbsData = [
    {
      label: formatMessage({ id: "sidebar.sources" }),
      to: `${breadcrumbBasePath}/`,
    },
    { label: formatMessage({ id: "sources.newSource" }) },
  ];

  if (!enterpriseSource) {
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
              href={appendConnectorTracking(enterpriseSource.name)}
              className={styles.button}
            >
              <FormattedMessage id="credits.talkToSales" />
              <Icon type="share" />
            </ExternalLink>
          </FlexContainer>
        </FlexContainer>
      </div>

      <div className={styles.rightSection}>
        {enterpriseSource && (
          <EnterpriseDocumentationPanel
            id={enterpriseSource.id}
            name={enterpriseSource.name}
            documentationUrl={enterpriseSource.url}
          />
        )}
      </div>
    </div>
  );
};
