import { useState } from "react";
import { v4 as uuid } from "uuid";

type LogoAnimationProps = JSX.IntrinsicElements["svg"] & {
  title?: string;
  titleId?: string;
};

export const LogoAnimation: React.FC<LogoAnimationProps> = ({ title, titleId, ...props }) => {
  const [myId] = useState(() => uuid());
  return (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      width={60}
      height={60}
      fill="none"
      viewBox="0 0 250 250"
      aria-labelledby={titleId}
      {...props}
    >
      {title ? <title id={titleId}>{title}</title> : null}
      <style>
        {
          "@keyframes tentacle1reveal{5%{stroke-dashoffset:460}80%,to{stroke-dashoffset:50}}@keyframes tentacle2reveal{0%,46%{stroke-dashoffset:150}85%,to{stroke-dashoffset:35}}@keyframes tentacle3reveal{0%,40%{stroke-dashoffset:250}80%,to{stroke-dashoffset:500}}@keyframes logoFadeOut{90%{opacity:1}to{opacity:0}}.tentacle{stroke-width:40px;stroke-linecap:round;fill:none;stroke:#615eff;stroke-opacity:1}"
        }
      </style>
      <clipPath id={`clipBody-${myId}`}>
        <path d="M137.06 36.521c-18.438.753-36.385 8.738-49.113 23.038a70.294 70.294 0 0 0-7.918 10.896l-54.855 94.356a19.121 19.121 0 0 0 14.412 1.878 18.996 18.996 0 0 0 11.535-8.785l21.213-36.578a68.427 68.427 0 0 0 10.523 23.545L48.24 204.445a19.095 19.095 0 0 0 14.414 1.883 19.036 19.036 0 0 0 6.647-3.254 18.92 18.92 0 0 0 4.885-5.535l31.886-54.875a49.987 49.987 0 0 1-16.664-37.87 50.008 50.008 0 0 1 12.903-32.876c14.803-16.48 39.297-21.269 59.335-11.686 26.706 12.773 36.44 45.214 21.89 70.24l-54.62 93.829a19.116 19.116 0 0 0 14.41 1.879 18.998 18.998 0 0 0 11.537-8.785l45.116-77.487c20.055-34.482 6.617-79.185-30.22-96.734-10.396-4.953-21.636-7.104-32.698-6.653z" />
        <path d="M158.895 82.987c12.247 6.635 17.482 23.253 11.124 35.673-16.788 29.177-33.882 58.186-50.788 87.298-3.018 5.666-6.832 11.57-13.444 13.341-4.626 1.545-12.415 1.289-14.985-2.18l48.515-83.459c-13.644-2.517-23.797-16.514-21.767-30.272 1.42-13.848 14.69-24.942 28.548-24.084 4.48.17 8.912 1.445 12.797 3.683zM139.76 99.948c-6.416 4.37-2.879 15.677 5 15.453 9.115.857 12.437-13.212 3.858-16.47-2.851-1.31-6.378-.905-8.858 1.017z" />
      </clipPath>
      <clipPath id={`clipEye-${myId}`}>
        <path d="M158.895 82.987c13.21 7.577 17.774 24.427 10.13 37.54 0 0-2.396 5.638-10.003 10.07-8.8 5.128-19.705 3.063-19.705 3.063a27.778 27.778 0 0 1-10.825-4.847 27.545 27.545 0 0 1-7.783-8.907 27.344 27.344 0 0 1-3.307-11.326c-.265-3.97.34-7.95 1.776-11.66a27.454 27.454 0 0 1 6.533-9.846 27.703 27.703 0 0 1 10.087-6.222 27.858 27.858 0 0 1 23.097 2.135zM139.76 99.948a8.645 8.645 0 0 0-2.232 2.53h-.003a8.565 8.565 0 0 0 .632 9.555 8.68 8.68 0 0 0 4.097 2.915 8.738 8.738 0 0 0 5.036.163 8.692 8.692 0 0 0 4.279-2.642 8.59 8.59 0 0 0 2.079-4.558 8.563 8.563 0 0 0-.821-4.938 8.645 8.645 0 0 0-3.444-3.652 8.72 8.72 0 0 0-9.623.627z" />
      </clipPath>
      <clipPath id={`clipTentacle3-${myId}`}>
        <path d="m169.025 120.527-52.603 90.251a18.997 18.997 0 0 1-11.536 8.784 19.122 19.122 0 0 1-14.412-1.878l48.843-84.024c13.31-22.196 21.618-25.35 29.708-13.133z" />
      </clipPath>
      <g
        style={{
          animation: "logoFadeOut infinite var(--duration)",
          animationDelay: "var(--delay)",
          // the typescript defintions for SVG's style attribute does not allow variable declarations
          // eslint-disable-next-line @typescript-eslint/ban-ts-comment
          // @ts-ignore-next-line
          "--delay": "200ms",
          "--duration": "2s",
        }}
      >
        <path
          d="M136.325 235.264s61.453-95.94 69.615-119.372c9.335-44.822-30.431-107.184-100.693-60.749-4.982 5.673-23.868 39.953-23.868 39.953l-52.4 85.457"
          className="tentacle"
          clipPath={`url(#clipBody-${myId})`}
          style={{
            strokeDasharray: 460,
            strokeDashoffset: 460,
            animation: "tentacle1reveal infinite var(--duration)",
            animationTimingFunction: "cubic-bezier(.25,.72,.61,.92)",
            animationDelay: "var(--delay)",
          }}
        />
        <path
          d="M83.315 92.38s7.327 37.902 17.618 44.694c-5.983 12.159-41.905 73.793-41.905 73.793"
          className="tentacle"
          clipPath={`url(#clipBody-${myId})`}
          style={{
            strokeDasharray: 150,
            strokeDashoffset: 150,
            animation: "tentacle2reveal infinite var(--duration)",
            animationDelay: "var(--delay)",
          }}
        />
        <path
          d="M92.724 230.379s56.245-95.366 72.608-122.61"
          className="tentacle"
          clipPath={`url(#clipTentacle3-${myId})`}
          style={{
            strokeWidth: 23,
            strokeDasharray: 250,
            strokeDashoffset: 250,
            animation: "tentacle3reveal infinite var(--duration)",
            animationTimingFunction: "cubic-bezier(.52,.39,.49,.29)",
            animationDelay: "var(--delay)",
          }}
          transform="translate(-7 6)"
        />
        <path
          d="M92.724 230.379s56.245-95.366 72.608-122.61c.02-10.735-13.783-26.564-30.163-18.53-16.38 8.033-15.448 42.918 20.323 35.648"
          className="tentacle"
          clipPath={`url(#clipEye-${myId})`}
          style={{
            strokeWidth: 23,
            strokeDasharray: 250,
            strokeDashoffset: 250,
            animation: "tentacle3reveal infinite var(--duration)",
            animationTimingFunction: "cubic-bezier(.52,.39,.49,.29)",
            animationDelay: "var(--delay)",
          }}
          transform="translate(-7 6)"
        />
      </g>
    </svg>
  );
};
