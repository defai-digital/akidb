// ESM wrapper for the CJS napi-rs loader.
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const require = createRequire(join(__dirname, "index.cjs"));
const binding = require("./index.cjs");

export const { RustIndex, AkiDbEngine } = binding;
