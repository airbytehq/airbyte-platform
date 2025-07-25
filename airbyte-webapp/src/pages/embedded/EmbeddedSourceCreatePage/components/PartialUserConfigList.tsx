import { useListPartialUserConfigs } from "core/api";

import { SelectableList } from "./SelectableList";

interface ConfigListProps {
  workspaceId: string;
  onSelectConfig: (partialUserConfigId: string) => void;
}

export const PartialUserConfigList: React.FC<ConfigListProps> = ({ workspaceId, onSelectConfig }) => {
  const { data: partialUserConfigs } = useListPartialUserConfigs(workspaceId);

  const items = partialUserConfigs
    .sort((a, b) => a.config_template_name.localeCompare(b.config_template_name))
    .map((config) => ({
      id: config.id,
      name: config.config_template_name,
      icon: config.config_template_icon ?? "",
      configured: true,
    }));
  return <SelectableList items={items} onSelect={onSelectConfig} />;
};
