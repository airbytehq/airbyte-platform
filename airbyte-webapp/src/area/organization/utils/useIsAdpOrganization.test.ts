import { renderHook } from "@testing-library/react";

import { useOrganization } from "core/api";

import { useCurrentOrganizationId } from "./useCurrentOrganizationId";
import { useIsAdpOrganization } from "./useIsAdpOrganization";

jest.mock("core/api");
jest.mock("./useCurrentOrganizationId");

const mockUseOrganization = useOrganization as jest.MockedFunction<typeof useOrganization>;
const mockUseCurrentOrganizationId = useCurrentOrganizationId as jest.MockedFunction<typeof useCurrentOrganizationId>;

const mockOrganizationId = "test-org-id";

const createMockOrganization = (isAgentic?: boolean) => ({
  organizationId: mockOrganizationId,
  organizationName: "Test Organization",
  email: "test@example.com",
  isAgentic,
});

describe("useIsAdpOrganization", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockUseCurrentOrganizationId.mockReturnValue(mockOrganizationId);
  });

  it("returns true when organization.isAgentic is true", () => {
    mockUseOrganization.mockReturnValue(createMockOrganization(true));

    const { result } = renderHook(() => useIsAdpOrganization());

    expect(result.current).toBe(true);
  });

  it("returns false when organization.isAgentic is false", () => {
    mockUseOrganization.mockReturnValue(createMockOrganization(false));

    const { result } = renderHook(() => useIsAdpOrganization());

    expect(result.current).toBe(false);
  });

  it("returns false when organization.isAgentic is undefined", () => {
    mockUseOrganization.mockReturnValue(createMockOrganization(undefined));

    const { result } = renderHook(() => useIsAdpOrganization());

    expect(result.current).toBe(false);
  });
});
