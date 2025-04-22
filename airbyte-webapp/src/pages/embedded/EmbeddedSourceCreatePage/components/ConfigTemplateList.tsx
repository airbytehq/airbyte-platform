import { FormattedMessage } from "react-intl";

import { EmptyState } from "components/EmptyState";
import { Box } from "components/ui/Box";

import { useListConfigTemplates } from "core/api";

import { SelectableList } from "./SelectableList";
import { useEmbeddedSourceParams } from "../hooks/useEmbeddedSourceParams";

export const ConfigTemplateSelectList: React.FC = () => {
  const { workspaceId, setSelectedTemplate } = useEmbeddedSourceParams();

  const onTemplateSelect = (templateId: string) => {
    setSelectedTemplate(templateId);
  };

  const { configTemplates } = useListConfigTemplates(workspaceId);
  if (configTemplates.length === 1) {
    setSelectedTemplate(configTemplates[0].id);
  }

  const items = configTemplates.map((template) => ({
    id: template.id,
    name: template.name,
    icon: template.icon,
  }));

  return (
    <SelectableList
      items={items}
      onSelect={onTemplateSelect}
      title={<FormattedMessage id="onboarding.sourceSetUp" />}
      emptyState={
        <Box mt="2xl" pt="2xl">
          <EmptyState text={<FormattedMessage id="configTemplates.emptyState" />} />
        </Box>
      }
    />
  );
};
