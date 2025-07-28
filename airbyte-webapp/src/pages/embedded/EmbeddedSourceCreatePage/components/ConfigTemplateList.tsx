import { FormattedMessage } from "react-intl";

import { EmptyState } from "components/EmptyState";
import { Box } from "components/ui/Box";

import { useListPartialUserConfigs } from "core/api";
import { SourceTemplateListItem } from "core/api/types/SonarClient";

import { SelectableList } from "./SelectableList";
import { useEmbeddedSourceParams } from "../hooks/useEmbeddedSourceParams";

export const ConfigTemplateSelectList: React.FC<{ configTemplates: SourceTemplateListItem[] }> = ({
  configTemplates,
}) => {
  const { workspaceId, setSelectedTemplate } = useEmbeddedSourceParams();

  const onTemplateSelect = (templateId: string) => {
    setSelectedTemplate(templateId);
  };

  const { data: partialUserConfigs } = useListPartialUserConfigs(workspaceId);

  const items = configTemplates.map((template) => ({
    id: template.id,
    name: template.name,
    icon: template.icon ?? undefined,
    configured: partialUserConfigs.some((config) => config.summarized_source_template.id === template.id),
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
