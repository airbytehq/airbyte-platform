import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";

import { useCurrentOrganizationId } from "area/organization/utils/useCurrentOrganizationId";

import { checkDomainVerification, createDomainVerification, listDomainVerifications } from "../generated/AirbyteClient";
import { SCOPE_ORGANIZATION } from "../scopes";
import { useRequestOptions } from "../useRequestOptions";

/**
 * Query keys for domain verification queries.
 * Follows the pattern: [scope, feature, ...identifiers]
 */
export const domainVerificationKeys = {
  all: [SCOPE_ORGANIZATION, "domainVerification"] as const,
  list: (organizationId: string) => [...domainVerificationKeys.all, "list", organizationId] as const,
};

/**
 * Hook to list all domain verifications for the current organization.
 *
 * Results are cached for 30 seconds to reduce unnecessary API calls, as domain
 * verification status only changes when the background cron job runs (~15 min intervals).
 *
 * @returns React Query result with list of domain verifications
 *
 * @example
 * const { data, isLoading, error } = useListDomainVerifications();
 * const domains = data?.domainVerifications || [];
 */
export const useListDomainVerifications = () => {
  const organizationId = useCurrentOrganizationId();
  const requestOptions = useRequestOptions();

  return useQuery({
    queryKey: domainVerificationKeys.list(organizationId),
    queryFn: async () => {
      const response = await listDomainVerifications({ organizationId }, requestOptions);
      // Sort by createdAt to maintain consistent order regardless of status updates
      return {
        ...response,
        domainVerifications: response.domainVerifications.sort((a, b) => a.createdAt - b.createdAt),
      };
    },
    enabled: !!organizationId,
    staleTime: 30_000, // 30 seconds - domains don't change frequently
  });
};

/**
 * Hook to create a new domain verification.
 *
 * This mutation creates a domain verification record and generates DNS TXT record
 * instructions. The domain is immediately saved with PENDING status and will be
 * verified by a background cron job.
 *
 * @returns React Query mutation result
 *
 * @example
 * // Using mutateAsync (promise-based):
 * const { mutateAsync: createDomain, isLoading } = useCreateDomainVerification();
 *
 * const response = await createDomain("example.com");
 * console.log(response.dnsRecordName);  // "_airbyte-verification.example.com"
 * console.log(response.dnsRecordValue); // "airbyte-domain-verification=abc123..."
 *
 * @example
 * // Using mutate (callback-based):
 * const { mutate: createDomain } = useCreateDomainVerification();
 *
 * createDomain("example.com", {
 *   onSuccess: (response) => {
 *     console.log("Created:", response.domain);
 *   },
 *   onError: (error) => {
 *     console.error("Failed:", error);
 *   }
 * });
 */
export const useCreateDomainVerification = () => {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();
  const organizationId = useCurrentOrganizationId();

  return useMutation({
    mutationFn: (domain: string) => createDomainVerification({ organizationId, domain }, requestOptions),
    onSuccess: () => {
      // Invalidate the list query to refresh the domain list
      queryClient.invalidateQueries({ queryKey: domainVerificationKeys.list(organizationId) });
    },
  });
};

/**
 * Hook to manually check a domain verification.
 *
 * Triggers an immediate DNS verification check instead of waiting for the cron job.
 * Useful for users who want instant feedback after adding their DNS record.
 *
 * @returns React Query mutation result
 *
 * @example
 * const { mutate: checkNow, isLoading } = useCheckDomainVerification();
 *
 * <Button onClick={() => checkNow(domainId)} isLoading={isLoading}>
 *   Check Now
 * </Button>
 */
export const useCheckDomainVerification = () => {
  const requestOptions = useRequestOptions();
  const queryClient = useQueryClient();
  const organizationId = useCurrentOrganizationId();

  return useMutation({
    mutationFn: (domainVerificationId: string) => checkDomainVerification({ domainVerificationId }, requestOptions),
    onSuccess: () => {
      // Invalidate the list query to refresh the domain list with updated status
      queryClient.invalidateQueries({ queryKey: domainVerificationKeys.list(organizationId) });
    },
  });
};
