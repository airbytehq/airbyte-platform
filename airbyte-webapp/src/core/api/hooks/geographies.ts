import { useCurrentWorkspace } from "./workspaces";
import { webBackendListGeographies } from "../generated/AirbyteClient";
import { SCOPE_USER } from "../scopes";
import { useRequestOptions } from "../useRequestOptions";
import { useSuspenseQuery } from "../useSuspenseQuery";

export function useAvailableGeographies() {
  const requestOptions = useRequestOptions();
  const workspace = useCurrentWorkspace();

  return useSuspenseQuery([SCOPE_USER, "geographies", "list"], () =>
    webBackendListGeographies({ organizationId: workspace.organizationId }, requestOptions)
  );
}
