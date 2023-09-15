import { useState } from "react";
import { FormattedMessage, useIntl } from "react-intl";
import { useLocation } from "react-router-dom";
import { useUpdateEffect } from "react-use";
import { match } from "ts-pattern";

import { LoadingPage } from "components";
import { Box } from "components/ui/Box";
import { Markdown } from "components/ui/Markdown";
import { Tabs } from "components/ui/Tabs";
import { ButtonTab } from "components/ui/Tabs/ButtonTab";

import { isSourceDefinition } from "core/domain/connector/source";
import { isCloudApp } from "core/utils/app";
import { links } from "core/utils/links";
import { useExperiment } from "hooks/services/Experiment";
import { EMBEDDED_DOCS_PATH, useDocumentation } from "hooks/services/useDocumentation";
import { useDocumentationPanelContext } from "views/Connector/ConnectorDocumentationLayout/DocumentationPanelContext";

import styles from "./DocumentationPanel.module.scss";
import { ResourceNotAvailable } from "./ResourceNotAvailable";

const OSS_ENV_MARKERS = /<!-- env:oss -->([\s\S]*?)<!-- \/env:oss -->/gm;
const CLOUD_ENV_MARKERS = /<!-- env:cloud -->([\s\S]*?)<!-- \/env:cloud -->/gm;

export const prepareMarkdown = (markdown: string, env: "oss" | "cloud"): string => {
  return env === "oss" ? markdown.replaceAll(CLOUD_ENV_MARKERS, "") : markdown.replaceAll(OSS_ENV_MARKERS, "");
};
type TabsType = "setupGuide" | "schema" | "erd";

const ImgRelativePathReplacer: React.FC<React.ImgHTMLAttributes<HTMLImageElement>> = ({ src, alt, ...props }) => {
  const newSrc = src && src.startsWith("../../") ? src.replace("../../", `${EMBEDDED_DOCS_PATH}/`) : src;
  return <img src={newSrc} alt={alt} {...props} />;
};

const LinkRelativePathReplacer: React.FC<React.AnchorHTMLAttributes<HTMLAnchorElement>> = ({
  href,
  children,
  ...props
}) => {
  // Relative URLs pointing to another place within the documentation.
  if (href && href.startsWith("../../")) {
    // In links replace with a link to the external documentation instead
    // The external path is the markdown URL without the "../../" prefix and the .md extension
    const docPath = href.replace(/^\.\.\/\.\.\/(.*?)(\.md)?$/, "$1");
    const docLink = `${links.docsLink}/${docPath}`;
    return (
      <a {...props} href={docLink} target="_blank" rel="noreferrer">
        {children}
      </a>
    );
  } else if (href && href.startsWith("#")) {
    return (
      <a {...props} href={href}>
        {children}
      </a>
    );
  }

  return (
    <a {...props} href={href} target="_blank" rel="noreferrer">
      {children}
    </a>
  );
};

export const DocumentationPanel: React.FC = () => {
  const { formatMessage } = useIntl();
  const { setDocumentationPanelOpen, documentationUrl, selectedConnectorDefinition } = useDocumentationPanelContext();
  const sourceType =
    selectedConnectorDefinition &&
    "sourceType" in selectedConnectorDefinition &&
    selectedConnectorDefinition.sourceType;
  const { supportLevel } = selectedConnectorDefinition || {};
  const showRequestSchemaButton = useExperiment("connector.showRequestSchemabutton", false) && sourceType === "api";
  const [isSchemaRequested, setIsSchemaRequested] = useState(false);
  const [isERDRequested, setIsERDRequested] = useState(false);

  const { data: docs, isLoading, error } = useDocumentation(documentationUrl, supportLevel);

  const location = useLocation();

  useUpdateEffect(() => {
    setDocumentationPanelOpen(false);
  }, [setDocumentationPanelOpen, location.pathname, location.search]);

  const [activeTab, setActiveTab] = useState<TabsType>("setupGuide");
  const tabs: Array<{ id: TabsType; name: JSX.Element }> = [
    {
      id: "setupGuide",
      name: <FormattedMessage id="sources.documentationPanel.tabs.setupGuide" />,
    },
    {
      id: "schema",
      name: <FormattedMessage id="sources.documentationPanel.tabs.schema" />,
    },
    {
      id: "erd",
      name: <FormattedMessage id="sources.documentationPanel.tabs.erd" />,
    },
  ];

  return isLoading || documentationUrl === "" ? (
    <LoadingPage />
  ) : (
    <div className={styles.container}>
      {selectedConnectorDefinition && isSourceDefinition(selectedConnectorDefinition) && showRequestSchemaButton && (
        <Box pt="md" pl="lg">
          <Tabs>
            {tabs.map((tabItem) => {
              return (
                <ButtonTab
                  id={tabItem.id}
                  key={tabItem.id}
                  name={tabItem.name}
                  isActive={activeTab === tabItem.id}
                  onSelect={(val) => {
                    setActiveTab(val as TabsType);
                  }}
                />
              );
            })}
          </Tabs>
        </Box>
      )}

      {match(activeTab)
        .with("setupGuide", () => (
          <Markdown
            className={styles.content}
            content={
              docs && !error
                ? prepareMarkdown(docs, isCloudApp() ? "cloud" : "oss")
                : formatMessage({ id: "connector.setupGuide.notFound" })
            }
            options={{
              overrides: {
                img: {
                  component: ImgRelativePathReplacer,
                },
                a: {
                  component: LinkRelativePathReplacer,
                },
              },
            }}
          />
        ))
        .with("schema", () => (
          <ResourceNotAvailable
            activeTab="schema"
            setRequested={setIsSchemaRequested}
            isRequested={isSchemaRequested}
          />
        ))
        .with("erd", () => (
          <ResourceNotAvailable activeTab="erd" setRequested={setIsERDRequested} isRequested={isERDRequested} />
        ))
        .exhaustive()}
    </div>
  );
};
