import { useListPartialUserConfigs } from "core/api";

import { SelectableList } from "./SelectableList";

interface ConfigListProps {
  workspaceId: string;
  onSelectConfig: (partialUserConfigId: string) => void;
}

export const PartialUserConfigList: React.FC<ConfigListProps> = ({ workspaceId, onSelectConfig }) => {
  const { data: partialUserConfigs } = useListPartialUserConfigs(workspaceId);

  const items = partialUserConfigs
    .sort((a, b) => a.summarized_source_template.name.localeCompare(b.summarized_source_template.name))
    .map((config) => ({
      id: config.id,
      name: config.summarized_source_template.name,
      icon: config.summarized_source_template.icon ?? "",
      configured: true,
    }));
  return <SelectableList items={items} onSelect={onSelectConfig} />;
};
