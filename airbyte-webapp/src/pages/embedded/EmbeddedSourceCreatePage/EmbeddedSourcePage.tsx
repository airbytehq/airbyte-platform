import { FormattedMessage } from "react-intl";

import { Button } from "components/ui/Button";
import { FlexContainer } from "components/ui/Flex";

import { useListPartialUserConfigs } from "core/api";

import { ConfigTemplateSelectList } from "./components/ConfigTemplateList";
import { PartialUserConfigCreateForm } from "./components/PartialUserConfigCreateForm";
import { PartialUserConfigEditForm } from "./components/PartialUserConfigEditForm";
import { PartialUserConfigList } from "./components/PartialUserConfigList";
import styles from "./EmbeddedSourcePage.module.scss";
import { useEmbeddedSourceParams } from "./hooks/useEmbeddedSourceParams";
export const EmbeddedSourceCreatePage: React.FC = () => {
  const {
    workspaceId,
    selectedTemplateId,
    selectedPartialConfigId,
    createPartialUserConfig,
    setCreateConfig,
    setSelectedConfig,
  } = useEmbeddedSourceParams();

  const { partialUserConfigs } = useListPartialUserConfigs(workspaceId);

  if (selectedTemplateId) {
    return (
      <EmbeddedSourcePageWrapper>
        <PartialUserConfigCreateForm />
      </EmbeddedSourcePageWrapper>
    );
  }

  if (selectedPartialConfigId) {
    return (
      <EmbeddedSourcePageWrapper>
        <PartialUserConfigEditForm />
      </EmbeddedSourcePageWrapper>
    );
  }

  if (!!createPartialUserConfig || partialUserConfigs.length === 0) {
    return (
      <EmbeddedSourcePageWrapper>
        <ConfigTemplateSelectList />
      </EmbeddedSourcePageWrapper>
    );
  }

  // default view: list of partial user configs with an "add" button
  return (
    <EmbeddedSourcePageWrapper>
      <FlexContainer direction="column" className={styles.wrapper}>
        <FlexContainer alignItems="flex-end">
          <Button onClick={setCreateConfig} icon="plus">
            <FormattedMessage id="sources.newSource" />
          </Button>
        </FlexContainer>
        <PartialUserConfigList workspaceId={workspaceId} onSelectConfig={setSelectedConfig} />
      </FlexContainer>
    </EmbeddedSourcePageWrapper>
  );
};

export const EmbeddedSourcePageWrapper: React.FC<React.PropsWithChildren> = ({ children }) => {
  return <div className={styles.wrapper}>{children}</div>;
};
