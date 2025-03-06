import type { Tag } from "../generated/AirbyteClient.schemas";

import { useMutation, useQueryClient } from "@tanstack/react-query";

import { listTags, updateTag, deleteTag, createTag } from "../generated/AirbyteClient";
import { SCOPE_WORKSPACE } from "../scopes";
import { useRequestOptions } from "../useRequestOptions";
import { useSuspenseQuery } from "../useSuspenseQuery";

export const tagKeys = {
  all: [SCOPE_WORKSPACE, "tags"] as const,
  lists: () => [...tagKeys.all, "list"] as const,
  list: (workspaceId: string) => [...tagKeys.lists(), workspaceId] as const,
  detail: (workspaceId: string, tagId: string) => [...tagKeys.all, "details", workspaceId, tagId] as const,
};

export const getTagsListQueryKey = (workspaceId: string) => {
  return tagKeys.list(workspaceId);
};

export const useGetTagsListQuery = (workspaceId: string) => {
  const requestOptions = useRequestOptions();
  return () => listTags({ workspaceId }, requestOptions);
};

export const useTagsList = (workspaceId: string) => {
  const queryKey = getTagsListQueryKey(workspaceId);
  const queryFn = useGetTagsListQuery(workspaceId);

  return useSuspenseQuery(queryKey, queryFn, {
    staleTime: 30_000, // 30 sec
  });
};

export const useCreateTag = () => {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();

  return useMutation((values: Parameters<typeof createTag>[0]) => createTag(values, requestOptions), {
    onSuccess: (newTag, variables) => {
      // Update tags list cache by adding the new tag
      queryClient.setQueryData<Tag[]>(tagKeys.list(variables.workspaceId), (oldTags) => {
        if (!oldTags) {
          return [newTag];
        }
        return [...oldTags, newTag];
      });
    },
  });
};

export const useUpdateTag = () => {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();

  return useMutation((values: Parameters<typeof updateTag>[0]) => updateTag(values, requestOptions), {
    onSuccess: (updatedTag, variables) => {
      // Update tags list cache
      queryClient.setQueryData<Tag[]>(tagKeys.list(variables.workspaceId), (oldTags) => {
        if (!oldTags) {
          return [updatedTag];
        }
        return oldTags.map((tag) => (tag.tagId === updatedTag.tagId ? updatedTag : tag));
      });
    },
  });
};

export const useDeleteTag = () => {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();

  return useMutation((values: Parameters<typeof deleteTag>[0]) => deleteTag(values, requestOptions), {
    onSuccess: (_, variables) => {
      // Update tags list cache by removing the deleted tag
      queryClient.setQueryData<Tag[]>(tagKeys.list(variables.workspaceId), (oldTags) => {
        if (!oldTags) {
          return [];
        }
        return oldTags.filter((tag) => tag.tagId !== variables.tagId);
      });
    },
  });
};
