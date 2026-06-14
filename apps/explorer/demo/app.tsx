import { mountWcolDemo } from "./game/use-wcol-demo.tsx";

const root = document.getElementById("root");
if (!root) throw new Error("#root not found");
mountWcolDemo(root);
