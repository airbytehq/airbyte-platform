// This file is used to mock svg files (imported with ?react) in jest tests
// https://react-svgr.com/docs/jest/

// eslint-disable-next-line import/no-anonymous-default-export
const MockSvg = (props) => <div {...props} data-testid="mocksvg" />;

export default MockSvg;
