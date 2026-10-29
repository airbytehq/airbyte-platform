import { render, screen } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router-dom";

import { useIsCloudApp } from "core/utils/app";

import MainLayout from "./MainLayout";

jest.mock("area/layout/SideBar", () => ({ SideBar: () => <div data-testid="sidebar" /> }));

jest.mock("cloud/components/CloudBannerGroup", () => ({
  CloudBannerGroup: () => <div data-testid="cloud-banner-group" />,
}));

jest.mock("cloud/components/AdpOrganizationAccessGuard", () => ({
  AdpOrganizationAccessGuard: ({ children }: React.PropsWithChildren) => <>{children}</>,
}));

jest.mock("core/api/", () => ({ usePrefetchOrganizationSummaries: () => () => undefined }));

jest.mock("core/errors", () => ({
  DefaultErrorBoundary: ({ children }: React.PropsWithChildren) => <>{children}</>,
  ForbiddenErrorBoundary: ({ children }: React.PropsWithChildren) => <>{children}</>,
}));

jest.mock("core/utils/app", () => ({ useIsCloudApp: jest.fn() }));

const renderLayout = () =>
  render(
    <MemoryRouter>
      <Routes>
        <Route element={<MainLayout />}>
          <Route index element={<div data-testid="page" />} />
        </Route>
      </Routes>
    </MemoryRouter>
  );

describe("MainLayout", () => {
  describe("in Cloud", () => {
    beforeEach(() => {
      jest.mocked(useIsCloudApp).mockReturnValue(true);
      renderLayout();
    });

    it("should render the cloud banner group", () => {
      expect(screen.getByTestId("cloud-banner-group")).toBeInTheDocument();
    });

    it("should render the cloud banner group above the sidebar", () => {
      const position = screen.getByTestId("cloud-banner-group").compareDocumentPosition(screen.getByTestId("sidebar"));

      expect(Boolean(position & Node.DOCUMENT_POSITION_FOLLOWING)).toBe(true);
    });

    it("should still render the page", () => {
      expect(screen.getByTestId("page")).toBeInTheDocument();
    });
  });

  describe("outside Cloud", () => {
    beforeEach(() => {
      jest.mocked(useIsCloudApp).mockReturnValue(false);
      renderLayout();
    });

    it("should not render the cloud banner group", () => {
      expect(screen.queryByTestId("cloud-banner-group")).not.toBeInTheDocument();
    });

    it("should still render the page", () => {
      expect(screen.getByTestId("page")).toBeInTheDocument();
    });
  });
});
