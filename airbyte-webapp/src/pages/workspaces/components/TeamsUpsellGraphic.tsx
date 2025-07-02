import React from "react";

import teamsUpsellGraphic from "./teams-upsell-graphic.png";

interface TeamsUpsellGraphicProps {
  width?: number | string;
  height?: number | string;
  className?: string;
  "data-testid"?: string;
}

export const TeamsUpsellGraphic: React.FC<TeamsUpsellGraphicProps> = ({
  width = 595,
  height = 444,
  className,
  "data-testid": dataTestId,
}) => {
  return (
    <img
      src={teamsUpsellGraphic}
      alt="Teams upsell graphic"
      width={width}
      height={height}
      className={className}
      data-testid={dataTestId}
    />
  );
};

export default TeamsUpsellGraphic;
