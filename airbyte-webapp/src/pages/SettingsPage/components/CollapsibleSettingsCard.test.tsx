import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { CollapsibleSettingsCard } from "./CollapsibleSettingsCard";

describe("CollapsibleSettingsCard", () => {
  it("renders the label with the body hidden by default", () => {
    render(
      <CollapsibleSettingsCard label="Set up SSO">
        <div>card body</div>
      </CollapsibleSettingsCard>
    );

    expect(screen.getByText("Set up SSO")).toBeInTheDocument();
    expect(screen.queryByText("card body")).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Set up SSO" })).toHaveAttribute("aria-expanded", "false");
  });

  it("renders the status node when provided", () => {
    render(
      <CollapsibleSettingsCard label="Set up SSO" status={<span>Optional</span>}>
        <div>card body</div>
      </CollapsibleSettingsCard>
    );

    expect(screen.getByText("Optional")).toBeInTheDocument();
  });

  it("renders the docs link only when docsLink is provided", () => {
    const { container, rerender } = render(
      <CollapsibleSettingsCard
        label="Set up SSO"
        docsLink="https://docs.airbyte.com/sso"
        docsLinkLabel="Check out our docs"
      />
    );
    expect(screen.getByRole("link")).toHaveAttribute("href", "https://docs.airbyte.com/sso");
    expect(screen.getByText("Check out our docs")).toBeInTheDocument();
    expect(container.querySelector("[data-icon='arrow-right']")).toBeInTheDocument();

    rerender(<CollapsibleSettingsCard label="Set up SSO" />);
    expect(screen.queryByRole("link")).not.toBeInTheDocument();
  });

  it("toggles body visibility when the header is clicked", async () => {
    render(
      <CollapsibleSettingsCard label="Set up SSO">
        <div>card body</div>
      </CollapsibleSettingsCard>
    );

    expect(screen.queryByText("card body")).not.toBeInTheDocument();

    await userEvent.click(screen.getByRole("button", { name: "Set up SSO" }));
    expect(screen.getByText("card body")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Set up SSO" })).toHaveAttribute("aria-expanded", "true");

    await userEvent.click(screen.getByRole("button", { name: "Set up SSO" }));
    await waitFor(() => expect(screen.queryByText("card body")).not.toBeInTheDocument());
  });

  it("respects defaultOpen", () => {
    render(
      <CollapsibleSettingsCard label="Set up SSO" defaultOpen>
        <div>card body</div>
      </CollapsibleSettingsCard>
    );

    expect(screen.getByText("card body")).toBeInTheDocument();
  });
});
