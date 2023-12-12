import { webBackendListGeographies } from "../generated/AirbyteClient";
import { SCOPE_USER } from "../scopes";
import { useRequestOptions } from "../useRequestOptions";
import { useSuspenseQuery } from "../useSuspenseQuery";

export function useAvailableGeographies() {
  const requestOptions = useRequestOptions();

  return useSuspenseQuery([SCOPE_USER, "geographies", "list"], () => webBackendListGeographies(requestOptions));
}
