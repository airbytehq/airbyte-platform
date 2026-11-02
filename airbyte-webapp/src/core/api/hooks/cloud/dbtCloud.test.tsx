import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook } from "@testing-library/react";
import { ReactNode } from "react";

import { mocked } from "test-utils";

import { WorkspaceRead } from "core/api/types/AirbyteClient";

import { useDbtCloudServiceToken } from "./dbtCloud";
import { useCurrentWorkspace, useUpdateWorkspace } from "../workspaces";

jest.mock("../workspaces", () => ({ useCurrentWorkspace: jest.fn(), useUpdateWorkspace: jest.fn() }));

describe("useDbtCloudServiceToken", () => {
  const updateWorkspace = jest.fn();
  let queryClient: QueryClient;
  const dbtConfig = { id: "dbt-id", name: "dbt cloud", customDbtHost: "custom.getdbt.com" };
  const otherConfig = { id: "other-id", name: "other integration" };
  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  );

  beforeEach(() => {
    jest.clearAllMocks();
    queryClient = new QueryClient({ defaultOptions: { mutations: { retry: false } } });
    mocked(useCurrentWorkspace).mockReturnValue({
      workspaceId: "workspace-id",
      webhookConfigs: [dbtConfig, otherConfig],
    } as WorkspaceRead);
    mocked(useUpdateWorkspace).mockReturnValue({ mutateAsync: updateWorkspace } as unknown as ReturnType<
      typeof useUpdateWorkspace
    >);
    updateWorkspace.mockResolvedValue({});
  });

  afterEach(() => queryClient.clear());

  it("rotates the token with the existing ID and preserves other webhook configurations", async () => {
    const { result } = renderHook(useDbtCloudServiceToken, { wrapper });
    await result.current.saveToken({ authToken: "new-token", accessUrl: "new.getdbt.com" });

    expect(updateWorkspace).toHaveBeenCalledWith({
      workspaceId: "workspace-id",
      webhookConfigs: [{ ...dbtConfig, authToken: "new-token", customDbtHost: "new.getdbt.com" }, otherConfig],
    });
  });

  it("removes only the dbt configuration", async () => {
    const { result } = renderHook(useDbtCloudServiceToken, { wrapper });
    await result.current.deleteToken();

    expect(updateWorkspace).toHaveBeenCalledWith({ workspaceId: "workspace-id", webhookConfigs: [otherConfig] });
  });

  it("creates a dbt configuration alongside unrelated configurations", async () => {
    mocked(useCurrentWorkspace).mockReturnValue({
      workspaceId: "workspace-id",
      webhookConfigs: [otherConfig],
    } as WorkspaceRead);
    const { result } = renderHook(useDbtCloudServiceToken, { wrapper });
    await result.current.saveToken({ authToken: "first-token" });

    expect(updateWorkspace).toHaveBeenCalledWith({
      workspaceId: "workspace-id",
      webhookConfigs: [otherConfig, { name: "dbt cloud", authToken: "first-token" }],
    });
  });
});
