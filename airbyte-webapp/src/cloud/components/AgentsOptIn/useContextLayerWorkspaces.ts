import { useMemo, useState } from "react";

import { useCurrentOrganizationId } from "area/organization/utils";
import { useListWorkspacesInOrganization } from "core/api";

const WORKSPACE_PAGE_SIZE = 25;

export interface ContextLayerWorkspace {
  workspaceId: string;
  workspaceName: string;
}

/**
 * Shows the organization's workspaces one page at a time. Cached workspaces are shown before the next page is fetched.
 */
export const useContextLayerWorkspaces = () => {
  const organizationId = useCurrentOrganizationId();
  const workspacesQuery = useListWorkspacesInOrganization({
    organizationId,
    pagination: { pageSize: WORKSPACE_PAGE_SIZE, rowOffset: 0 },
  });
  const [visibleWorkspaceCount, setVisibleWorkspaceCount] = useState(WORKSPACE_PAGE_SIZE);
  const { data, fetchNextPage, hasNextPage } = workspacesQuery;

  const allWorkspaces = useMemo<ContextLayerWorkspace[]>(() => {
    const apiWorkspaces = data?.pages.flatMap((page) => page.workspaces ?? []) ?? [];
    return apiWorkspaces.map((workspace) => ({
      workspaceId: workspace.workspaceId,
      workspaceName: workspace.name,
    }));
  }, [data?.pages]);
  const workspaces = allWorkspaces.slice(0, visibleWorkspaceCount);
  const hasMoreWorkspaces = allWorkspaces.length > visibleWorkspaceCount || hasNextPage;
  const loadMoreWorkspaces = async () => {
    if (allWorkspaces.length <= visibleWorkspaceCount && hasNextPage) {
      const nextPage = await fetchNextPage();
      if (nextPage.isError) {
        return;
      }
    }
    setVisibleWorkspaceCount((count) => count + WORKSPACE_PAGE_SIZE);
  };

  return { workspacesQuery, workspaces, hasMoreWorkspaces, loadMoreWorkspaces };
};
