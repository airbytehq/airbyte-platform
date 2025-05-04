import { useListPartialUserConfigs } from "core/api";

import { SelectableList } from "./SelectableList";

interface ConfigListProps {
  workspaceId: string;
  onSelectConfig: (partialUserConfigId: string) => void;
}

export const PartialUserConfigList: React.FC<ConfigListProps> = ({ workspaceId, onSelectConfig }) => {
  const { partialUserConfigs } = useListPartialUserConfigs(workspaceId);

  const items = partialUserConfigs
    .sort((a, b) => a.configTemplateName.localeCompare(b.configTemplateName))
    .map((config) => ({
      id: config.partialUserConfigId,
      name: config.configTemplateName,
      icon: config.configTemplateIcon,
      configured: true,
    }));
  return <SelectableList items={items} onSelect={onSelectConfig} />;
};
