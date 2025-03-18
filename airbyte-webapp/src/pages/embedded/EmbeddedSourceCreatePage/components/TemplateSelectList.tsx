import { useSearchParams } from "react-router-dom";

import { ConnectorIcon } from "components/ConnectorIcon";
import { Box } from "components/ui/Box";

import { useListConfigTemplates } from "core/api";

import styles from "./TemplateSelectList.module.scss";

export const TemplateSelectList: React.FC = () => {
  const [searchParams, setSearchParams] = useSearchParams();
  const organizationId = searchParams.get("organizationId");

  const onTemplateSelect = (templateId: string) => {
    setSearchParams((params) => {
      params.set("selectedTemplateId", templateId);
      return params;
    });
  };

  const { configTemplates } = useListConfigTemplates(organizationId ?? "");

  return (
    <ul className={styles.list}>
      {configTemplates.map((template) => {
        return (
          <li key={template.id}>
            <Box py="sm">
              <button className={styles.button} onClick={() => onTemplateSelect(template.id)}>
                <ConnectorIcon icon={template.icon} />
                {template.name}
              </button>
            </Box>
          </li>
        );
      })}
    </ul>
  );
};
