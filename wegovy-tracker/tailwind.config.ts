import type { Config } from "tailwindcss";

const config: Config = {
  content: ["./app/**/*.{ts,tsx}", "./components/**/*.{ts,tsx}"],
  darkMode: "class",
  theme: {
    extend: {
      colors: {
        surface: {
          DEFAULT: "#0b0f14",
          raised: "#131a22",
          border: "#232c37",
        },
      },
    },
  },
  plugins: [],
};

export default config;
