import { ACTIONS } from "./constants";
import { UserEvent } from "./types";

export const createHtmlElement = ({
  tagName,
  attributes,
  styles = {},
}: {
  tagName: keyof HTMLElementTagNameMap;
  attributes?: Partial<HTMLElementTagNameMap[keyof HTMLElementTagNameMap]>;
  styles?: Partial<CSSStyleDeclaration>;
}) => {
  const element = document.createElement(tagName);

  if (attributes) {
    Object.entries(attributes).forEach(([key, value]) => {
      if (key === "textContent") {
        element.textContent = value;
      } else {
        element.setAttribute(key, value);
      }
    });
  }
  if (styles) {
    Object.assign(element.style, styles);
  }

  return element;
};

export const getIframeContainer = (doc: Document | null, action: UserEvent["action"]) => {
  if (action === ACTIONS.helpCenterShown) {
    return doc?.querySelector(`[data-embed='helpCenterForm']`)?.firstChild;
  }
  if (action === ACTIONS.contactFormShown) {
    return doc?.querySelector(`[data-embed='ticketSubmissionForm']`)?.firstChild?.firstChild;
  }
  return null;
};

const createStatusIndicator = () => {
  return createHtmlElement({
    tagName: "span",
    attributes: {
      textContent: "·",
    },
    styles: {
      display: "inline-block",
      fontSize: "50px",
      lineHeight: "0.2",
      verticalAlign: "text-top",
      marginRight: "4px",
    },
  });
};

export const createMessageElement = ({ message }: { message: string }) => {
  const statusIndicator = createStatusIndicator();
  const element = createHtmlElement({
    tagName: "p",
    styles: {
      margin: "0",
    },
  });

  element.appendChild(statusIndicator);
  element.appendChild(document.createTextNode(message));
  return element;
};

export const createStatusPageLink = ({ url, color }: { url: string; color: string }) => {
  return createHtmlElement({
    tagName: "a",
    attributes: {
      target: "_blank",
      textContent: "Go to status page",
      href: url,
    },
    styles: {
      color,
    },
  });
};
