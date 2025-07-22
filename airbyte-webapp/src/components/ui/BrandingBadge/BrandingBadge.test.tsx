import { render, screen } from "@testing-library/react";
import React from "react";
import { IntlProvider } from "react-intl";

import { BrandingBadge } from "./BrandingBadge";

const renderWithIntl = (component: React.ReactElement) => {
  return render(
    <IntlProvider locale="en" messages={{}}>
      {component}
    </IntlProvider>
  );
};

describe("BrandingBadge", () => {
  it("renders enterprise badge", () => {
    renderWithIntl(<BrandingBadge product="enterprise" testId="test-enterprise" />);
    expect(screen.getByTestId("test-enterprise")).toBeInTheDocument();
  });

  it("renders cloudForTeams badge", () => {
    renderWithIntl(<BrandingBadge product="cloudForTeams" testId="test-teams" />);
    expect(screen.getByTestId("test-teams")).toBeInTheDocument();
  });

  it("renders cloudInTrial badge", () => {
    renderWithIntl(<BrandingBadge product="cloudInTrial" testId="test-trial" />);
    expect(screen.getByTestId("test-trial")).toBeInTheDocument();
  });

  it("renders nothing when product is null", () => {
    const { container } = renderWithIntl(<BrandingBadge product={null} />);
    expect(container.firstChild).toBeNull();
  });
});
