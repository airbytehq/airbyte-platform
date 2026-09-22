import React from "react";
import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { Card } from "components/ui/Card";
import { CopyButton } from "components/ui/CopyButton";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { ExternalLink } from "components/ui/Link";
import { LoadingPage } from "components/ui/LoadingPage";
import { Pre } from "components/ui/Pre";
import { Text } from "components/ui/Text";

import { useAgentsProvisioningStatus } from "core/api";
import { useIsCloudApp } from "core/utils/app";
import { links } from "core/utils/links";

import styles from "./InstallMcpPage.module.scss";
import {
  buildClaudeCodeCommand,
  buildCursorConfig,
  buildCursorDeeplink,
  buildVsCodeCliCommand,
  buildVsCodeConfig,
  buildVsCodeDeeplink,
  buildVsCodeInsidersDeeplink,
  CLOUD_MCP_URL,
} from "./mcpInstallConfigs";
import { useShowAgentsOptIn } from "./useShowAgentsOptIn";

const CodeBlock: React.FC<{ content: string }> = ({ content }) => (
  <div className={styles.codeBlock}>
    <Pre>{content}</Pre>
    <CopyButton content={content} className={styles.codeCopyButton}>
      <FormattedMessage id="cloud.installMcp.copy" />
    </CopyButton>
  </div>
);

const InstallCard: React.FC<{
  titleId: string;
  clientsId: string;
  descriptionId: string;
  children: React.ReactNode;
}> = ({ titleId, clientsId, descriptionId, children }) => (
  <Card className={styles.card}>
    <FlexContainer direction="column" gap="lg">
      <Heading as="h2" size="sm">
        <FormattedMessage id={titleId} />
      </Heading>
      <Text size="sm" color="grey">
        <FormattedMessage id={clientsId} />
      </Text>
      <Text size="sm">
        <FormattedMessage id={descriptionId} />
      </Text>
      {children}
    </FlexContainer>
  </Card>
);

const Divider: React.FC = () => (
  <FlexContainer alignItems="center" gap="md" className={styles.divider}>
    <span className={styles.dividerLine} />
    <Text size="xs" color="grey">
      <FormattedMessage id="cloud.installMcp.or" />
    </Text>
    <span className={styles.dividerLine} />
  </FlexContainer>
);

const Label: React.FC<{ id: string }> = ({ id }) => (
  <Text size="sm" bold>
    <FormattedMessage id={id} />
  </Text>
);

const openInstallLink = (href: string, external = false) => {
  if (external) {
    window.open(href, "_blank", "noopener,noreferrer");
    return;
  }
  window.open(href, "_blank");
};

const InstallMcpPageContent: React.FC = () => {
  return (
    <div className={styles.page}>
      <FlexContainer direction="column" gap="xl">
        <div className={styles.header}>
          <Heading as="h1" size="lg">
            <FormattedMessage id="cloud.installMcp.title" />
          </Heading>
          <Text color="grey">
            <FormattedMessage id="cloud.installMcp.subtitle" />{" "}
            <ExternalLink href={links.agentsDocs} opensInNewTab>
              <FormattedMessage id="cloud.installMcp.documentation" />
            </ExternalLink>
          </Text>
        </div>
        <Text bold>
          <FormattedMessage id="cloud.installMcp.chooseMethod" />
        </Text>
        <InstallCard
          titleId="cloud.installMcp.pasteUrl.title"
          clientsId="cloud.installMcp.pasteUrl.clients"
          descriptionId="cloud.installMcp.pasteUrl.description"
        >
          <CodeBlock content={CLOUD_MCP_URL} />
        </InstallCard>
        <Divider />
        <InstallCard
          titleId="cloud.installMcp.oneClick.title"
          clientsId="cloud.installMcp.oneClick.clients"
          descriptionId="cloud.installMcp.oneClick.description"
        >
          <FlexContainer gap="md" wrap="wrap">
            <Button
              type="button"
              data-testid="installMcp-cursor"
              onClick={() => openInstallLink(buildCursorDeeplink(CLOUD_MCP_URL))}
            >
              <FormattedMessage id="cloud.installMcp.oneClick.cursor" />
            </Button>
            <Button
              type="button"
              data-testid="installMcp-vscode"
              onClick={() => openInstallLink(buildVsCodeDeeplink(CLOUD_MCP_URL), true)}
            >
              <FormattedMessage id="cloud.installMcp.oneClick.vscode" />
            </Button>
            <Button
              type="button"
              data-testid="installMcp-vscode-insiders"
              onClick={() => openInstallLink(buildVsCodeInsidersDeeplink(CLOUD_MCP_URL), true)}
            >
              <FormattedMessage id="cloud.installMcp.oneClick.vscodeInsiders" />
            </Button>
          </FlexContainer>
        </InstallCard>
        <Divider />
        <InstallCard
          titleId="cloud.installMcp.cli.title"
          clientsId="cloud.installMcp.cli.clients"
          descriptionId="cloud.installMcp.cli.description"
        >
          <FlexContainer direction="column" gap="md">
            <Label id="cloud.installMcp.cli.claudeLabel" />
            <CodeBlock content={buildClaudeCodeCommand(CLOUD_MCP_URL)} />
            <Label id="cloud.installMcp.cli.vscodeLabel" />
            <CodeBlock content={buildVsCodeCliCommand(CLOUD_MCP_URL)} />
          </FlexContainer>
        </InstallCard>
        <Divider />
        <InstallCard
          titleId="cloud.installMcp.jsonConfig.title"
          clientsId="cloud.installMcp.jsonConfig.clients"
          descriptionId="cloud.installMcp.jsonConfig.description"
        >
          <FlexContainer direction="column" gap="md">
            <Label id="cloud.installMcp.jsonConfig.vscodeLabel" />
            <CodeBlock content={buildVsCodeConfig(CLOUD_MCP_URL)} />
            <Label id="cloud.installMcp.jsonConfig.cursorLabel" />
            <CodeBlock content={buildCursorConfig(CLOUD_MCP_URL)} />
          </FlexContainer>
        </InstallCard>
        <Text size="sm" color="grey">
          <FormattedMessage id="cloud.installMcp.footer" />{" "}
          <ExternalLink href={links.agentsDocs} opensInNewTab>
            <FormattedMessage id="cloud.installMcp.documentation" />
          </ExternalLink>
        </Text>
      </FlexContainer>
    </div>
  );
};

export const InstallMcpPage: React.FC = () => {
  const isCloudApp = useIsCloudApp();
  const showAgentsOptIn = useShowAgentsOptIn();
  const status = useAgentsProvisioningStatus({ enabled: isCloudApp && showAgentsOptIn });

  if (!isCloudApp || !showAgentsOptIn || !status || (!status.is_enrolled && !status.external_cloud_eligible)) {
    return null;
  }

  return (
    <React.Suspense fallback={<LoadingPage />}>
      <InstallMcpPageContent />
    </React.Suspense>
  );
};
