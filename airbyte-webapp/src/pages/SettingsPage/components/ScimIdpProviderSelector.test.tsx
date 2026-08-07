import { screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { render } from "test-utils";

import { ScimIdpProvider } from "core/api/types/AirbyteClient";

import { ScimIdpProviderSelector } from "./ScimIdpProviderSelector";

describe("ScimIdpProviderSelector", () => {
  it("renders a labeled radiogroup with both provider options", async () => {
    await render(<ScimIdpProviderSelector value={undefined} onChange={jest.fn()} />);

    expect(screen.getByRole("radiogroup", { name: "Identity provider" })).toBeInTheDocument();
    expect(screen.getByRole("radio", { name: "Okta" })).toBeInTheDocument();
    expect(screen.getByRole("radio", { name: "Microsoft Entra ID" })).toBeInTheDocument();
  });

  it("renders neither segment checked when value is undefined", async () => {
    await render(<ScimIdpProviderSelector value={undefined} onChange={jest.fn()} />);

    expect(screen.getByRole("radio", { name: "Okta" })).toHaveAttribute("aria-checked", "false");
    expect(screen.getByRole("radio", { name: "Microsoft Entra ID" })).toHaveAttribute("aria-checked", "false");
  });

  it("checks the stored provider when a value is provided", async () => {
    await render(<ScimIdpProviderSelector value={ScimIdpProvider.microsoft_entra_id} onChange={jest.fn()} />);

    expect(screen.getByRole("radio", { name: "Microsoft Entra ID" })).toHaveAttribute("aria-checked", "true");
    expect(screen.getByRole("radio", { name: "Okta" })).toHaveAttribute("aria-checked", "false");
  });

  it("calls onChange with the wire value when a segment is clicked", async () => {
    const handleChange = jest.fn();
    await render(<ScimIdpProviderSelector value={undefined} onChange={handleChange} />);

    await userEvent.click(screen.getByRole("radio", { name: "Okta" }));
    expect(handleChange).toHaveBeenCalledWith("okta");

    await userEvent.click(screen.getByRole("radio", { name: "Microsoft Entra ID" }));
    expect(handleChange).toHaveBeenCalledWith("microsoft_entra_id");
  });

  it("clears the selection when value is reset to undefined", async () => {
    const handleChange = jest.fn();
    const { rerender } = await render(<ScimIdpProviderSelector value={undefined} onChange={handleChange} />);

    await userEvent.click(screen.getByRole("radio", { name: "Okta" }));
    rerender(<ScimIdpProviderSelector value={ScimIdpProvider.okta} onChange={handleChange} />);
    expect(screen.getByRole("radio", { name: "Okta" })).toHaveAttribute("aria-checked", "true");

    rerender(<ScimIdpProviderSelector value={undefined} onChange={handleChange} />);
    expect(screen.getByRole("radio", { name: "Okta" })).toHaveAttribute("aria-checked", "false");
  });

  it("moves the selection with arrow-key navigation", async () => {
    const handleChange = jest.fn();
    await render(<ScimIdpProviderSelector value={undefined} onChange={handleChange} />);

    // Nothing is checked yet, so the first option (Okta) is the roving tabindex target.
    await userEvent.tab();
    expect(screen.getByRole("radio", { name: "Okta" })).toHaveFocus();

    await userEvent.keyboard("{ArrowRight}");
    expect(handleChange).toHaveBeenCalledWith("microsoft_entra_id");

    handleChange.mockClear();
    await userEvent.keyboard("{ArrowLeft}");
    expect(handleChange).toHaveBeenCalledWith("okta");
  });

  it("does not call onChange when disabled", async () => {
    const handleChange = jest.fn();
    await render(<ScimIdpProviderSelector value={ScimIdpProvider.okta} onChange={handleChange} disabled />);

    await userEvent.click(screen.getByRole("radio", { name: "Microsoft Entra ID" }));

    expect(handleChange).not.toHaveBeenCalled();
  });
});
