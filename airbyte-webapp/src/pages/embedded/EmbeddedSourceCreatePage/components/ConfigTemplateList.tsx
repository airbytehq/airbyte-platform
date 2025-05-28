import { FormattedMessage } from "react-intl";

import { EmptyState } from "components/EmptyState";
import { Box } from "components/ui/Box";

import { useListConfigTemplates, useListPartialUserConfigs } from "core/api";

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
  const { partialUserConfigs } = useListPartialUserConfigs(workspaceId);

  const items = configTemplates.map((template) => ({
    id: template.id,
    name: template.name,
    icon: template.icon,
    configured: partialUserConfigs.some((config) => config.configTemplateId === template.id),
  }));

  return (
    <SelectableList
      items={items}
      onSelect={onTemplateSelect}
      emptyState={
        <Box mt="2xl" pt="2xl">
          <EmptyState text={<FormattedMessage id="configTemplates.emptyState" />} />
        </Box>
      }
    />
  );
};
