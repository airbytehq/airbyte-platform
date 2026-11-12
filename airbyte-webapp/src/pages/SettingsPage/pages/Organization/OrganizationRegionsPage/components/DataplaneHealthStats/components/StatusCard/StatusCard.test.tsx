import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { render } from "test-utils";

import { StatusCard, StatusCardProps } from "./StatusCard";

const renderCard = (props: Partial<StatusCardProps>) =>
  render(<StatusCard label="Secrets manager reachable" level="success" value={{ text: "Yes" }} {...props} />);

describe("StatusCard", () => {
  describe("without details", () => {
    beforeEach(async () => {
      await renderCard({});
    });

    it("shows no details link", () => {
      expect(screen.queryByRole("button")).not.toBeInTheDocument();
    });
  });

  describe("with collapsed details", () => {
    beforeEach(async () => {
      await renderCard({ details: { isExpanded: false, onToggle: jest.fn(), panelId: "details-panel" } });
    });

    it("offers to view the details", () => {
      expect(screen.getByRole("button", { name: "View details" })).toBeInTheDocument();
    });

    it("marks the link as collapsed", () => {
      expect(screen.getByRole("button", { name: "View details" })).toHaveAttribute("aria-expanded", "false");
    });

    it("links to the panel it controls", () => {
      expect(screen.getByRole("button", { name: "View details" })).toHaveAttribute("aria-controls", "details-panel");
    });
  });

  describe("with expanded details", () => {
    beforeEach(async () => {
      await renderCard({ details: { isExpanded: true, onToggle: jest.fn(), panelId: "details-panel" } });
    });

    it("offers to hide the details", () => {
      expect(screen.getByRole("button", { name: "Hide details" })).toBeInTheDocument();
    });

    it("marks the link as expanded", () => {
      expect(screen.getByRole("button", { name: "Hide details" })).toHaveAttribute("aria-expanded", "true");
    });
  });

  describe("when the details link is clicked", () => {
    const onToggle = jest.fn();

    beforeEach(async () => {
      onToggle.mockClear();
      await renderCard({ details: { isExpanded: false, onToggle, panelId: "details-panel" } });
      await userEvent.click(screen.getByRole("button", { name: "View details" }));
    });

    it("toggles the details once", () => {
      expect(onToggle).toHaveBeenCalledTimes(1);
    });
  });
});
