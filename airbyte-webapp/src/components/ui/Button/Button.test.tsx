import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import React from "react";

import { Button } from "./Button";
import styles from "./Button.module.scss";
import { ButtonProps } from "./types";

describe("Button", () => {
  it("renders with default props", () => {
    render(<Button>Click me</Button>);
    const button = screen.getByRole("button", { name: "Click me" });
    expect(button).toBeInTheDocument();
    expect(button).toHaveClass(styles.button);
    expect(button).toHaveClass(styles["button--size-xs"]);
    expect(button).toHaveClass(styles["button--primary"]);
  });

  it("renders with different variants", () => {
    const variantClassMap = {
      primary: "button--primary",
      secondary: "button--secondary",
      danger: "button--danger",
      magic: "button--magic",
      clear: "button--clear",
      clearDark: "button--clear-dark",
      primaryDark: "button--primary-dark",
      secondaryDark: "button--secondary-dark",
      link: "button--link",
    } as const;

    Object.entries(variantClassMap).forEach(([variant, className]) => {
      const { unmount } = render(<Button variant={variant as ButtonProps["variant"]}>Button</Button>);
      const button = screen.getByRole("button", { name: "Button" });
      expect(button).toHaveClass(styles[className]);
      unmount();
    });
  });

  it("renders with different sizes", () => {
    const sizeMap = {
      xs: "button--size-xs",
      sm: "button--size-sm",
    } as const;

    Object.entries(sizeMap).forEach(([size, className]) => {
      const { unmount } = render(<Button size={size as "xs" | "sm"}>Button</Button>);
      const button = screen.getByRole("button", { name: "Button" });
      expect(button).toHaveClass(styles[className]);
      unmount();
    });
  });

  it("renders with loading state", () => {
    render(<Button isLoading>Button</Button>);
    const button = screen.getByRole("button", { name: "Button" });
    expect(button).toHaveClass(styles["button--loading"]);
    expect(button).toBeDisabled();
  });

  it("renders with disabled state", () => {
    render(<Button disabled>Button</Button>);
    const button = screen.getByRole("button", { name: "Button" });
    expect(button).toBeDisabled();
  });

  it("renders with icon on the left", () => {
    render(
      <Button icon="cross" iconPosition="left">
        Button
      </Button>
    );
    const button = screen.getByRole("button", { name: "Button" });
    const icon = button.querySelector(`.${styles.button__icon}`);
    expect(icon).toHaveClass(styles["button__icon--left"]);
  });

  it("renders with icon on the right", () => {
    render(
      <Button icon="cross" iconPosition="right">
        Button
      </Button>
    );
    const button = screen.getByRole("button", { name: "Button" });
    const icon = button.querySelector(`.${styles.button__icon}`);
    expect(icon).toHaveClass(styles["button__icon--right"]);
  });

  it("renders with full width", () => {
    render(<Button full>Button</Button>);
    const button = screen.getByRole("button", { name: "Button" });
    expect(button).toHaveClass(styles["button--full-width"]);
  });

  it("renders with custom width", () => {
    render(<Button width={200}>Button</Button>);
    const button = screen.getByRole("button", { name: "Button" });
    expect(button).toHaveStyle({ width: "200px" });
  });

  it("handles click events", async () => {
    const handleClick = jest.fn();
    render(<Button onClick={handleClick}>Button</Button>);
    const button = screen.getByRole("button", { name: "Button" });
    await userEvent.click(button);
    expect(handleClick).toHaveBeenCalledTimes(1);
  });

  it("forwards ref", () => {
    const ref = React.createRef<HTMLButtonElement>();
    render(<Button ref={ref}>Button</Button>);
    expect(ref.current).toBeInstanceOf(HTMLButtonElement);
  });

  it("applies custom className", () => {
    render(<Button className="custom-class">Button</Button>);
    const button = screen.getByRole("button", { name: "Button" });
    expect(button).toHaveClass("custom-class");
  });

  it("renders with data-testid", () => {
    render(<Button data-testid="test-button">Button</Button>);
    const button = screen.getByTestId("test-button");
    expect(button).toBeInTheDocument();
  });
});
