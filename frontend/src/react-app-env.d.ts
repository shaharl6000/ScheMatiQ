/// <reference types="react-scripts" />

declare namespace NodeJS {
  interface ProcessEnv {
    REACT_APP_API_BASE?: string;
    REACT_APP_WS_BASE?: string;
  }
}