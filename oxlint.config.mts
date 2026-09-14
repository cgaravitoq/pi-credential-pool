import { defineConfig } from "oxlint";
import antiSlop from "ultracite/oxlint/anti-slop";

export default defineConfig({
	extends: [antiSlop],
	rules: {
		"anti-slop/no-runtime-typeof": "off", // boundary decoders use typeof
		"anti-slop/no-unknown-parameters": "off", // catch handlers and test doubles take unknown
		"anti-slop/no-unsafe-dictionary-type": "off", // payloads are dictionaries until parsed
		"anti-slop/require-safety-comment-for-type-assertion": "off", // no mandatory SAFETY comments
	},
});