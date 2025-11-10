import React from "react";
import { render, screen } from "@testing-library/react";
import "@testing-library/jest-dom";
import { SettingsLayout, SettingsLayoutContent } from "../src/area/settings/components/SettingsLayout";

// Mocks de dependencias
jest.mock("components/HeadTitle", () => ({
  HeadTitle: ({ titles }: any) => (
    <div data-testid="head-title">{JSON.stringify(titles)}</div>
  ),
}));

jest.mock("components/ui/Flex", () => ({
  FlexContainer: ({ children, className }: any) => (
    <div data-testid="flex-container" className={className}>
      {children}
    </div>
  ),
  FlexItem: ({ children, className }: any) => (
    <div data-testid="flex-item" className={className}>
      {children}
    </div>
  ),
}));

jest.mock("../src/pages/SettingsPage/SettingsLayout.module.scss", () => ({
  settings: "settings-class",
  settings__main: "settings-main-class",
  settings__content: "settings-content-class",
}));


describe("SettingsLayout", () => {
  it("renderiza correctamente el HeadTitle y los children", () => {
    render(
      <SettingsLayout>
        <div data-testid="child">Child content</div>
      </SettingsLayout>
    );

    // Verifica que HeadTitle se renderizo con el id correcto
    expect(screen.getByTestId("head-title")).toHaveTextContent("sidebar.settings");

    // Verifica la estructura del contenedor principal
    const flexContainer = screen.getByTestId("flex-container");
    expect(flexContainer).toHaveClass("settings-class");

    // Verifica que children aparece dentro del main
    const main = screen.getByRole("main");
    expect(main).toHaveClass("settings-main-class");
    expect(screen.getByTestId("child")).toBeInTheDocument();
  });
});

describe("SettingsLayoutContent", () => {
  it("renderiza correctamente su contenido y aplica la clase adecuada", () => {
    render(
      <SettingsLayoutContent>
        <div data-testid="inner">Inner content</div>
      </SettingsLayoutContent>
    );

    const flexItem = screen.getByTestId("flex-item");
    expect(flexItem).toHaveClass("settings-content-class");
    expect(screen.getByTestId("inner")).toBeInTheDocument();
  });
});
