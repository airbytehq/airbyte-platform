import { useListPartialUserConfigs } from "core/api";

import { SelectableList } from "./SelectableList";

interface ConfigListProps {
  workspaceId: string;
  onSelectConfig: (partialUserConfigId: string) => void;
}

export const PartialUserConfigList: React.FC<ConfigListProps> = ({ workspaceId, onSelectConfig }) => {
  const { partialUserConfigs } = useListPartialUserConfigs(workspaceId);

  const items = partialUserConfigs.map((config) => ({
    id: config.partialUserConfigId,
    name: config.configTemplateName,
    icon: config.configTemplateIcon,
  }));

  // no empty state because we redirect to the mask create form if there are no configs
  return <SelectableList items={items} onSelect={onSelectConfig} />;
};
