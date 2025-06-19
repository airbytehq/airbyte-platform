import { useMemo } from "react";
import { FormattedMessage, useIntl } from "react-intl";

import { LoadingPage } from "components/LoadingPage";
import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { ExternalLink } from "components/ui/Link";
import { Markdown } from "components/ui/Markdown";

import { useConnectorDocumentation } from "core/api";
import { useIsCloudApp } from "core/utils/app";
import { prepareMarkdown, removeFirstHeading } from "views/Connector/ConnectorDocumentationLayout/DocumentationPanel";
import styles from "views/Connector/ConnectorDocumentationLayout/DocumentationPanel.module.scss";

const remarkPlugins = [removeFirstHeading];

interface EnterpriseDocumentationPanelProps {
  id: string;
  name: string;
  documentationUrl: string;
}

export const EnterpriseDocumentationPanel: React.FC<EnterpriseDocumentationPanelProps> = ({
  // This is temporary solution for displaying enterprise connector docs.
  // The existing DocumentationPanel component is wrapped in context expectations that don't align
  // with the simplicity of our enterprise stubs implementation.
  // Once we are done with this experiment we can remove this component.
  id,
  name,
  documentationUrl,
}) => {
  const { formatMessage } = useIntl();
  const isCloudApp = useIsCloudApp();
  const { data, isLoading, error } = useConnectorDocumentation("source", id, undefined, documentationUrl);

  const doc = data?.doc;

  const docsContent = useMemo(
    () =>
      doc && !error
        ? prepareMarkdown(doc, isCloudApp ? "cloud" : "oss")
        : formatMessage({ id: "connector.setupGuide.notFound" }),
    [doc, error, formatMessage, isCloudApp]
  );

  if (isLoading) {
    return <LoadingPage />;
  }

  return (
    <FlexContainer className={styles.container} direction="column" gap="none">
      <FlexContainer alignItems="center" className={styles.header} justifyContent="space-between">
        <Heading as="h1">
          <FormattedMessage id="connector.setupGuide" />
        </Heading>
        <ExternalLink href={documentationUrl}>
          <Button variant="secondary" icon="share">
            {formatMessage({ id: "connector.setupGuide.fullDocs" })}
          </Button>
        </ExternalLink>
      </FlexContainer>
      <FlexContainer direction="column" justifyContent="space-between" className={styles.connectorDocumentationHeader}>
        <Heading as="h2" className={styles.enterpriseDocsHeader}>
          {name}
        </Heading>
      </FlexContainer>
      <Markdown className={styles.content} content={docsContent} options={{}} remarkPlugins={remarkPlugins} />
    </FlexContainer>
  );
};
