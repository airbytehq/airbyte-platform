import React from "react";
import { useIntl } from "react-intl";

import { Input } from "components/ui/Input";

import { useBulkEditService } from "hooks/services/BulkEdit/BulkEditService";

import styles from "./SyncCatalogStreamSearch.module.scss";

interface SyncCatalogStreamSearchProps {
  onSearch: (value: string) => void;
}

export const SyncCatalogStreamSearch: React.FC<SyncCatalogStreamSearchProps> = ({ onSearch }) => {
  const { formatMessage } = useIntl();
  const { isActive } = useBulkEditService();

  return (
    <div className={styles.container}>
      <Input
        disabled={isActive}
        className={styles.searchInput}
        placeholder={formatMessage({
          id: `form.nameSearch`,
        })}
        onChange={(e) => onSearch(e.target.value)}
      />
    </div>
  );
};
