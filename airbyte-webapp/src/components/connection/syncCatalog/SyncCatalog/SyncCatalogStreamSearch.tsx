import React from "react";
import { useIntl } from "react-intl";

import { Input } from "components/ui/Input";

import styles from "./SyncCatalogStreamSearch.module.scss";

interface SyncCatalogStreamSearchProps {
  onSearch: (value: string) => void;
}

export const SyncCatalogStreamSearch: React.FC<SyncCatalogStreamSearchProps> = ({ onSearch }) => {
  const { formatMessage } = useIntl();

  return (
    <div className={styles.container}>
      <Input
        className={styles.searchInput}
        placeholder={formatMessage({
          id: `form.nameSearch`,
        })}
        onChange={(e) => onSearch(e.target.value)}
      />
    </div>
  );
};
