export function getTextColorForBackground(hex: string): "dark" | "light" {
  if (!isSafeHexValue(hex)) {
    // Default to dark text
    return "dark";
  }

  // Convert to RGB
  const r = parseInt(hex.substring(0, 2), 16) / 255;
  const g = parseInt(hex.substring(2, 4), 16) / 255;
  const b = parseInt(hex.substring(4, 6), 16) / 255;

  // Apply the WCAG formula for relative luminance
  // https://www.w3.org/WAI/GL/wiki/Relative_luminance
  const sRGB = [r, g, b].map((c) => (c <= 0.04045 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4)));
  const luminance = 0.2126 * sRGB[0] + 0.7152 * sRGB[1] + 0.0722 * sRGB[2];

  // Calculate contrast ratio against white (1) and black (0). This is imperfect, because we don't use true black and
  // white for our text color variants. However, it's a good enough approximation for our use case.
  // https://www.w3.org/TR/WCAG20-TECHS/G17.html
  const contrastWhite = 1.05 / (luminance + 0.05);
  const contrastBlack = (luminance + 0.05) / 0.05;

  return contrastWhite >= contrastBlack ? "light" : "dark";
}

export function isSafeHexValue(color: string): boolean {
  return /^([A-Fa-f0-9]{6})$/.test(color);
}
