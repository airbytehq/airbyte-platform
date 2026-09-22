import { screen } from "@testing-library/react";
import { Route, Routes } from "react-router-dom";

import { render, TestWrapper } from "test-utils";

import { useCurrentOrganizationId } from "area/organization/utils";

import { ContextLayerPage } from "./ContextLayerPage";

jest.mock("area/organization/utils", () => ({
  useCurrentOrganizationId: jest.fn(() => "test-organization-id"),
}));

jest.mock("components/ui/HeadTitle", () => ({
  HeadTitle: ({ titles }: { titles: Array<{ id: string }> }) => (
    <div data-testid="head-title">{titles.map(({ id }) => id).join("|")}</div>
  ),
}));

const mockUseCurrentOrganizationId = useCurrentOrganizationId as jest.MockedFunction<typeof useCurrentOrganizationId>;

describe("ContextLayerPage", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockUseCurrentOrganizationId.mockReturnValue("test-organization-id");
  });

  it("owns the Context Layer secondary navigation and renders its content", async () => {
    await render(<ContextLayerPage />, {
      wrapper: ({ children }) => (
        <TestWrapper route="/organization/test-organization-id/context-layer">
          <Routes>
            <Route path="/organization/:organizationId/context-layer" element={children}>
              <Route index element={<div>Context Layer settings content</div>} />
            </Route>
          </Routes>
        </TestWrapper>
      ),
    });

    expect(screen.getByText("Context Layer")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Settings" })).toHaveAttribute(
      "href",
      "/organization/test-organization-id/context-layer"
    );
    expect(screen.getByRole("link", { name: "Settings" })).toHaveAttribute("aria-current", "page");
    expect(screen.getByText("Context Layer settings content")).toBeInTheDocument();
    expect(screen.getByTestId("head-title")).toHaveTextContent("cloud.contextLayer.navigation.title");
  });
});
