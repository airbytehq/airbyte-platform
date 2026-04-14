import { useMutation, useQueryClient } from "@tanstack/react-query";

import { useCurrentWorkspaceId } from "area/workspace/utils";

import { createPrivateLink, deletePrivateLink, listPrivateLinksForWorkspace } from "../generated/AirbyteClient";
import { PrivateLinkCreateRequestBody, PrivateLinkRead, PrivateLinkStatus } from "../generated/AirbyteClient.schemas";
import { SCOPE_WORKSPACE } from "../scopes";
import { useRequestOptions } from "../useRequestOptions";
import { useSuspenseQuery } from "../useSuspenseQuery";

export const privateLinkKeys = {
  all: [SCOPE_WORKSPACE, "privateLinks"] as const,
  lists: () => [...privateLinkKeys.all, "list"] as const,
  list: (workspaceId: string) => [...privateLinkKeys.lists(), workspaceId] as const,
};

const TERMINAL_STATUSES: ReadonlySet<PrivateLinkStatus> = new Set([
  PrivateLinkStatus.available,
  PrivateLinkStatus.create_failed,
  PrivateLinkStatus.delete_failed,
]);

export const useListPrivateLinks = () => {
  const requestOptions = useRequestOptions();
  const workspaceId = useCurrentWorkspaceId();

  return useSuspenseQuery(
    privateLinkKeys.list(workspaceId),
    () => listPrivateLinksForWorkspace({ workspaceId }, requestOptions),
    {
      refetchInterval: (data) => {
        const links = (data as { privateLinks: PrivateLinkRead[] } | undefined)?.privateLinks;
        const hasInProgress = links?.some((pl) => !TERMINAL_STATUSES.has(pl.status));
        return hasInProgress ? 5000 : false;
      },
    }
  );
};

export const useCreatePrivateLink = () => {
  const requestOptions = useRequestOptions();
  const workspaceId = useCurrentWorkspaceId();
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (data: Omit<PrivateLinkCreateRequestBody, "workspaceId">) =>
      createPrivateLink({ ...data, workspaceId }, requestOptions),
    onSuccess: (newLink) => {
      queryClient.setQueryData<{ privateLinks: PrivateLinkRead[] }>(privateLinkKeys.list(workspaceId), (old) => ({
        privateLinks: [...(old?.privateLinks ?? []), newLink],
      }));
    },
  });
};

export const useDeletePrivateLink = () => {
  const requestOptions = useRequestOptions();
  const workspaceId = useCurrentWorkspaceId();
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (privateLinkId: string) => deletePrivateLink({ privateLinkId, workspaceId }, requestOptions),
    onSuccess: (_, privateLinkId) => {
      queryClient.setQueryData<{ privateLinks: PrivateLinkRead[] }>(privateLinkKeys.list(workspaceId), (old) => ({
        privateLinks: (old?.privateLinks ?? []).map((pl) =>
          pl.id === privateLinkId ? { ...pl, status: "deleting" as PrivateLinkRead["status"] } : pl
        ),
      }));
    },
  });
};
