import { createGlobalStyle} from "styled-components"

export const GlobalStylesLight = createGlobalStyle`
  :root {
    --color-black: #000000;
    --color-white: #ffffff;

    --color-blue-30: #f4f4ff;
    --color-blue-40: #f0efff;
    --color-blue-50: #eae9ff;
    --color-blue-100: #cbc8ff;
    --color-blue-200: #a6a4ff;
    --color-blue-300: #7f7eff;
    --color-blue-400: #615eff;
    --color-blue-500: #433bfb;
    --color-blue-600: #3f30ee;
    --color-blue-700: #3622e1;
    --color-blue-800: #2e0ad7;
    --color-blue-900: #2800bd;

    --color-dark-blue-30: #f7f8fc;
    --color-dark-blue-40: #eff0f5;
    --color-dark-blue-50: #e6e7ef;
    --color-dark-blue-100: #c0c3d9;
    --color-dark-blue-200: #989dbf;
    --color-dark-blue-300: #989dbf;
    --color-dark-blue-400: #565c94;
    --color-dark-blue-500: #3b4283;
    --color-dark-blue-600: #353b7b;
    --color-dark-blue-700: #2d3270;
    --color-dark-blue-800: #262963;
    --color-dark-blue-900: #1a194d;
    --color-dark-blue-1000: #0a0a23;

    --color-grey-30: #fcfcfd;
    --color-grey-40: #fafafc;
    --color-grey-50: #f8f8fa;
    --color-grey-100: #e8e8ed;
    --color-grey-200: #d9d9e0;
    --color-grey-300: #afafc1;
    --color-grey-400: #8b8ba0;
    --color-grey-500: #717189;
    --color-grey-600: #595971;
    --color-grey-700: #494961;
    --color-grey-800: #35354a;
    --color-grey-900: #252536;

    --color-orange-50: #fae9e8;
    --color-orange-100: #fecbbf;
    --color-orange-200: #fea996;
    --color-orange-300: #fe866c;
    --color-orange-400: #ff6a4d;
    --color-orange-500: #ff4f31;
    --color-orange-600: #f4492d;
    --color-orange-700: #e64228;
    --color-orange-800: #d83c24;
    --color-orange-900: #bf2f1b;

    --color-green-30: #f4fcfd;
    --color-green-40: #f0fcfd;
    --color-green-50: #dcf6f8;
    --color-green-100: #a7e9ec;
    --color-green-200: #67dae1;
    --color-green-300: #00cbd6;
    --color-green-400: #00c0cd;
    --color-green-500: #00b5c7;
    --color-green-600: #00a5b5;
    --color-green-700: #00909b;
    --color-green-800: #007c84;
    --color-green-900: #005959;

    --color-red-30: #fff4f6;
    --color-red-40: #ffeff2;
    --color-red-50: #ffe4e8;
    --color-red-100: #ffbac6;
    --color-red-200: #ff8da1;
    --color-red-300: #ff5e7b;
    --color-red-400: #fb395f;
    --color-red-500: #f51a46;
    --color-red-600: #e51145;
    --color-red-700: #d00543;
    --color-red-800: #bc0042;
    --color-red-900: #99003f;

    --color-yellow-50: #fdf8e1;
    --color-yellow-100: #fbecb3;
    --color-yellow-200: #f9e081;
    --color-yellow-300: #f8d54e;
    --color-yellow-400: #f7ca26;
    --color-yellow-500: #f6c000;
    --color-yellow-600: #f6b300;
    --color-yellow-700: #f7a000;
    --color-yellow-800: #f79000;
    --color-yellow-900: #f77100;

    --color-overlay-background: rgba(26 25 77 / 50%);

    --box-shadow: 0 2px 4px rgba(26 25 77 / 12%);
    --box-shadow-left: -2px 0 10px rgba(26 25 77 / 12%);
    --box-shadow-right: 2px 0 10px rgba(26 25 77 / 12%);
    --box-shadow-raised: 0 10px 19px rgba(26 25 77 / 16%);
    --box-shadow-popup: 0 0 22px rgba(26 25 77 / 12%);
    --box-shadow-sidebar: 0 2px 4px rgba(26 25 77 / 5%);
  }
  `
export const GlobalStylesDark = createGlobalStyle`
  :root {
  --color-black: #ffffff;
  --color-white: #000000;

  --color-blue-30: #0b0a04;
  --color-blue-40: #141308;
  --color-blue-50: #1d1b0d;
  --color-blue-100: #3f41f9;
  --color-blue-200: #595cb0;
  --color-blue-300: #707d40;
  --color-blue-400: #9ea1ff;
  --color-blue-500: #bcc0a0;
  --color-blue-600: #c0cf11;
  --color-blue-700: #cde01e;
  --color-blue-800: #d8f831;
  --color-blue-900: #e2ff45;

  --color-dark-blue-30: #080703;
  --color-dark-blue-40: #0f0d09;
  --color-dark-blue-50: #17140f;
  --color-dark-blue-100: #3f3c26;
  --color-dark-blue-200: #676c40;
  --color-dark-blue-300: #676c40;
  --color-dark-blue-400: #a9a36b;
  --color-dark-blue-500: #c4bd7c;
  --color-dark-blue-600: #ccc484;
  --color-dark-blue-700: #d6d08f;
  --color-dark-blue-800: #ddd899;
  --color-dark-blue-900: #ebe3b2;
  --color-dark-blue-1000: #f5f7f5;

  --color-grey-30: #030303;
  --color-grey-40: #050505;
  --color-grey-50: #070707;
  --color-grey-100: #171717;
  --color-grey-200: #26261f;
  --color-grey-300: #59606e;
  --color-grey-400: #74756f;
  --color-grey-500: #8e8e76;
  --color-grey-600: #a6a68e;
  --color-grey-700: #b6b6a9;
  --color-grey-800: #c8c8e5;
  --color-grey-900: #dcdce9;

  --color-orange-50: #e4f616;
  --color-orange-100: #143440;
  --color-orange-200: #b16569;
  --color-orange-300: #017993;
  --color-orange-400: #ff957b;
  --color-orange-500: #ffaa8e;
  --color-orange-600: #0bb3ab;
  --color-orange-700: #19c8c3;
  --color-orange-800: #26dddb;
  --color-orange-900: #34f2f2;

  --color-green-30: #030b0c;
  --color-green-40: #040e10;
  --color-green-50: #031214;
  --color-green-100: #f8f616;
  --color-green-200: #98421e;
  --color-green-300: #b4e4e0;
  --color-green-400: #cfdbe1;
  --color-green-500: #eae2e1;
  --color-green-600: #f5dbda;
  --color-green-700: #f5b5b3;
  --color-green-800: #f5908c;
  --color-green-900: #f06b62;

  --color-red-30: #000b0d;
  --color-red-40: #001115;
  --color-red-50: #00161c;
  --color-red-100: #340539;
  --color-red-200: #711e51;
  --color-red-300: #a71678;
  --color-red-400: #e30b9c;
  --color-red-500: #ff0aaf;
  --color-red-600: #ff0ec5;
  --color-red-700: #ff16e0;
  --color-red-800: #ff1ffc;
  --color-red-900: #ff32ff;

  --color-yellow-50: #0d0700;
  --color-yellow-100: #040900;
  --color-yellow-200: #080c01;
  --color-yellow-300: #0c0f01;
  --color-yellow-400: #0e1101;
  --color-yellow-500: #111401;
  --color-yellow-600: #121502;
  --color-yellow-700: #141703;
  --color-yellow-800: #151903;
  --color-yellow-900: #171b03;

  --color-overlay-background: rgba(229, 230, 204, 50%);

  --box-shadow: 0 2px 4px rgba(229, 230, 204, 0.12%);
  --box-shadow-left: -2px 0 10px rgba(229, 230, 204, 12%);
  --box-shadow-right: 2px 0 10px rgba(229, 230, 204, 12%);
  --box-shadow-raised: 0 10px 19px rgba(229, 230, 204, 16%);
  --box-shadow-popup: 0 0 22px rgba(229, 230, 204, 12%);
  --box-shadow-sidebar: 0 2px 4px rgba(229, 230, 204, 5%);
}
  `