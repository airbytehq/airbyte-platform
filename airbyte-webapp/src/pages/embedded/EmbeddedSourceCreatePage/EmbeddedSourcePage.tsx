import { FormattedMessage } from "react-intl";

import { Box } from "components/ui/Box";
import { Card } from "components/ui/Card";
import { FlexContainer } from "components/ui/Flex";
import { Heading } from "components/ui/Heading";
import { Icon } from "components/ui/Icon";
import { ButtonTab, Tabs } from "components/ui/Tabs";
import { Text } from "components/ui/Text";

import { useListPartialUserConfigs } from "core/api";

import { ConfigTemplateSelectList } from "./components/ConfigTemplateList";
import { PartialUserConfigCreateForm } from "./components/PartialUserConfigCreateForm";
import { PartialUserConfigEditForm } from "./components/PartialUserConfigEditForm";
import { PartialUserConfigList } from "./components/PartialUserConfigList";
import styles from "./EmbeddedSourcePage.module.scss";
import { useEmbeddedSourceParams } from "./hooks/useEmbeddedSourceParams";

/**
 * The EmbeddedSourceCreatePage component is rendered inside of the embedded widget.
 * It allows users to create or edit partial user configurations.
 */
export const EmbeddedSourceCreatePage: React.FC = () => {
  const {
    workspaceId,
    selectedTemplateId,
    selectedPartialConfigId,
    setEditConfig,
    editPartialUserConfig,
    setSelectedConfig,
    clearSelectedConfig,
    clearSelectedTemplate,
  } = useEmbeddedSourceParams();

  const { partialUserConfigs } = useListPartialUserConfigs(workspaceId);

  if (selectedTemplateId) {
    return (
      <EmbeddedSourcePageWrapper>
        <EmbeddedSourcePageHeader
          headingMessage={
            <Box py="sm">
              <button onClick={() => clearSelectedTemplate()} className={styles.clearButton}>
                <FlexContainer gap="sm">
                  <Icon type="chevronLeft" size="sm" />
                  <Text as="span" size="md" bold color="grey400">
                    <FormattedMessage id="partialUserConfig.back" />
                  </Text>
                </FlexContainer>
              </button>
            </Box>
          }
        />
        <PartialUserConfigCreateForm />
      </EmbeddedSourcePageWrapper>
    );
  }

  if (selectedPartialConfigId) {
    return (
      <EmbeddedSourcePageWrapper>
        <EmbeddedSourcePageHeader
          headingMessage={
            <Box py="sm">
              <button onClick={() => clearSelectedConfig()} className={styles.clearButton}>
                <Icon type="chevronLeft" size="sm" />
                <Text as="span" size="sm" bold color="grey400">
                  <FormattedMessage id="partialUserConfig.back" />
                </Text>
              </button>
            </Box>
          }
        />
        <PartialUserConfigEditForm selectedPartialConfigId={selectedPartialConfigId} />
      </EmbeddedSourcePageWrapper>
    );
  }

  if (!editPartialUserConfig || partialUserConfigs.length === 0) {
    return (
      <EmbeddedSourcePageWrapper>
        <EmbeddedSourcePageHeader headingMessage={<FormattedMessage id="embeddedSource.selectIntegration" />} />
        {partialUserConfigs.length > 0 && (
          <Tabs>
            <ButtonTab
              id="create"
              name={<FormattedMessage id="partialUserConfig.create.tab" />}
              isActive
              onSelect={() => setEditConfig(false)}
            />
            <ButtonTab
              id="update"
              isActive={false}
              name={<FormattedMessage id="partialUserConfig.update.tab" />}
              onSelect={() => setEditConfig(true)}
            />
          </Tabs>
        )}
        <ConfigTemplateSelectList />
      </EmbeddedSourcePageWrapper>
    );
  }

  return (
    <EmbeddedSourcePageWrapper>
      <EmbeddedSourcePageHeader headingMessage={<FormattedMessage id="embeddedSource.selectIntegration" />} />
      <Tabs>
        <ButtonTab
          id="create"
          isActive={false}
          name={<FormattedMessage id="partialUserConfig.create.tab" />}
          onSelect={() => setEditConfig(false)}
        />
        <ButtonTab
          id="update"
          name={<FormattedMessage id="partialUserConfig.update.tab" />}
          isActive
          onSelect={() => setEditConfig(true)}
        />
      </Tabs>
      <PartialUserConfigList workspaceId={workspaceId} onSelectConfig={setSelectedConfig} />
    </EmbeddedSourcePageWrapper>
  );
};

export const EmbeddedSourcePageHeader: React.FC<{ headingMessage: React.ReactNode }> = ({ headingMessage }) => {
  const { allowedOrigin } = useEmbeddedSourceParams();

  return (
    <FlexContainer direction="row" alignItems="center" justifyContent="space-between">
      <Heading size="sm" as="h1">
        {headingMessage}
      </Heading>
      <button
        className={styles.clearButton}
        onClick={() => {
          if (!!allowedOrigin) {
            window.parent.postMessage("CLOSE_DIALOG", allowedOrigin);
          }
        }}
      >
        <Icon type="cross" size="lg" />
      </button>
    </FlexContainer>
  );
};

export const EmbeddedSourcePageWrapper: React.FC<React.PropsWithChildren> = ({ children }) => {
  return (
    <Card className={styles.wrapper} bodyClassName={styles.wrapper}>
      <FlexContainer className={styles.wrapper} direction="column" gap="lg">
        {children}
      </FlexContainer>
    </Card>
  );
};
