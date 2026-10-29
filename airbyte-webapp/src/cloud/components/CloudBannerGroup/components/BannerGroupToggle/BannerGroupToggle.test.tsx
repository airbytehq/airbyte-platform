import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { IntlProvider } from "react-intl";

import messages from "locales/en.json";

import { BannerGroupToggle } from "./BannerGroupToggle";

const renderToggle = (props: Partial<React.ComponentProps<typeof BannerGroupToggle>> = {}) => {
  const onToggle = jest.fn();
  render(
    <IntlProvider locale="en" messages={messages}>
      <BannerGroupToggle isCollapsed={false} hiddenCount={2} onToggle={onToggle} {...props} />
    </IntlProvider>
  );

  return { onToggle };
};

describe("BannerGroupToggle", () => {
  describe("when expanded", () => {
    it("should render the expanded toggle", () => {
      renderToggle({ isCollapsed: false });

      expect(screen.getByTestId("cloud-banner-group-expanded")).toBeInTheDocument();
    });

    it("should report itself as expanded", () => {
      renderToggle({ isCollapsed: false });

      expect(screen.getByRole("button")).toHaveAttribute("aria-expanded", "true");
    });

    it("should not show the hidden count", () => {
      renderToggle({ isCollapsed: false, hiddenCount: 5 });

      expect(screen.getByRole("button")).not.toHaveTextContent("5");
    });
  });

  describe("when collapsed", () => {
    it("should render the collapsed toggle", () => {
      renderToggle({ isCollapsed: true, hiddenCount: 3 });

      expect(screen.getByTestId("cloud-banner-group-collapsed")).toBeInTheDocument();
    });

    it("should report itself as collapsed", () => {
      renderToggle({ isCollapsed: true, hiddenCount: 3 });

      expect(screen.getByRole("button")).toHaveAttribute("aria-expanded", "false");
    });

    it("should show the number of hidden banners", () => {
      renderToggle({ isCollapsed: true, hiddenCount: 3 });

      expect(screen.getByRole("button")).toHaveTextContent("3");
    });
  });

  it.each([true, false])("should call onToggle once when clicked (isCollapsed: %s)", async (isCollapsed) => {
    const { onToggle } = renderToggle({ isCollapsed });

    await userEvent.click(screen.getByRole("button"));

    expect(onToggle).toHaveBeenCalledTimes(1);
  });

  it("should toggle from the keyboard", async () => {
    const { onToggle } = renderToggle({ isCollapsed: true });

    await userEvent.tab();
    await userEvent.keyboard("{Enter}");

    expect(onToggle).toHaveBeenCalledTimes(1);
  });
});
