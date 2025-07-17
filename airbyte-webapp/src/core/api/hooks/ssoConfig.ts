import { useMutation, useQueryClient, useQuery } from "@tanstack/react-query";

import { useCurrentOrganizationId } from "area/organization/utils/useCurrentOrganizationId";
import { SSOFormValues } from "pages/SettingsPage/UpdateSSOSettingsForm";

import { getSsoConfig, createSsoConfig, deleteSsoConfig } from "../generated/AirbyteClient";
import { CreateSSOConfigRequestBody, DeleteSSOConfigRequestBody } from "../generated/AirbyteClient.schemas";
import { SCOPE_ORGANIZATION } from "../scopes";
import { useRequestOptions } from "../useRequestOptions";

export const ssoConfigKeys = {
  all: [SCOPE_ORGANIZATION, "ssoConfig"] as const,
  detail: (organizationId: string) => [...ssoConfigKeys.all, organizationId] as const,
};

const useGetSsoConfigSafe = (organizationId: string) => {
  const requestOptions = useRequestOptions();
  return useQuery(ssoConfigKeys.detail(organizationId), () => getSsoConfig({ organizationId }, requestOptions), {
    enabled: !!organizationId,
    retry: false,
  });
};

export const useCreateSsoConfig = () => {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (ssoConfig: CreateSSOConfigRequestBody) => createSsoConfig(ssoConfig, requestOptions),
    onSuccess: (_, variables) => {
      queryClient.invalidateQueries(ssoConfigKeys.detail(variables.organizationId));
    },
  });
};

export const useDeleteSsoConfig = () => {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (deleteSsoConfigRequest: DeleteSSOConfigRequestBody) =>
      deleteSsoConfig(deleteSsoConfigRequest, requestOptions),
    onSuccess: (_, variables) => {
      queryClient.invalidateQueries(ssoConfigKeys.detail(variables.organizationId));
    },
  });
};

export const useSSOConfigManagement = () => {
  const queryClient = useQueryClient();
  const requestOptions = useRequestOptions();
  const organizationId = useCurrentOrganizationId();

  // Safe query that doesn't throw on 404
  const { data: ssoConfig, isLoading } = useGetSsoConfigSafe(organizationId);

  // Create mutation with optimistic update
  const createSsoConfigMutation = useMutation({
    mutationFn: (ssoConfigData: SSOFormValues) => createSsoConfig({ ...ssoConfigData, organizationId }, requestOptions),
    onSuccess: (data) => {
      // Optimistically update cache to immediately show configured state
      queryClient.setQueryData(ssoConfigKeys.detail(organizationId), data);
      queryClient.invalidateQueries(ssoConfigKeys.detail(organizationId));
    },
  });

  // Delete mutation that clears cache
  const deleteSsoConfigMutation = useMutation({
    mutationFn: () => {
      if (!ssoConfig?.companyIdentifier) {
        throw new Error("Cannot delete SSO config: companyIdentifier is required");
      }
      return deleteSsoConfig({ organizationId, companyIdentifier: ssoConfig.companyIdentifier }, requestOptions);
    },
    onSuccess: () => {
      // Remove from cache to immediately show unconfigured state
      queryClient.removeQueries(ssoConfigKeys.detail(organizationId));
    },
  });

  // Determine if SSO is configured
  const isSSOConfigured = !!(
    ssoConfig &&
    ssoConfig.clientId &&
    ssoConfig.clientSecret &&
    ssoConfig.companyIdentifier &&
    ssoConfig.emailDomains?.length
  );

  return {
    ssoConfig: ssoConfig || null,
    isSSOConfigured,
    isLoading: isLoading || createSsoConfigMutation.isLoading || deleteSsoConfigMutation.isLoading,
    createSsoConfig: createSsoConfigMutation.mutateAsync,
    deleteSsoConfig: deleteSsoConfigMutation.mutateAsync,
  };
};
