import { Row } from "@tanstack/react-table";

import { SyncCatalogUIModel } from "../SyncCatalogTable";

/**
 * Check if row is namespace
 */
export const isNamespaceRow = (row: Row<SyncCatalogUIModel>) =>
  row.depth === 0 && row.original.rowType === "namespace"; /**
 
/**
 * Get the root parent id, which is the namespace id
 */
export const getNamespaceRowId = (row: Row<SyncCatalogUIModel>) => row.id.split(".")[0];
