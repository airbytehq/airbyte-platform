import React from "react";
import "@testing-library/jest-dom";
import { render, screen } from "@testing-library/react";
import { SafeMarkdown } from "../src/components/chat/SafeMarkdown";


test("renderiza markdown correctamente", () => {
  const markdown = "**Texto en negrita**";
  render(<SafeMarkdown content={markdown} />);
  const boldElement = screen.getByText("Texto en negrita");
  expect(boldElement.tagName.toLowerCase()).toBe("strong");
});


test("muestra el fallback cuando ocurre un error al renderizar", () => {

  jest.mock("markdown-to-jsx", () => {
    throw new Error("Error en markdown");
  });


  const { SafeMarkdown } = require("../src/components/chat/SafeMarkdown");
  const content = "Texto simple";

  render(<SafeMarkdown content={content} />);
  expect(screen.getByText(content)).toBeInTheDocument();
});
