module.exports = {
	"env": {
		"commonjs": true,
		"es6": true,
		"node": true
	},
	"extends": [
		"airbnb-base",
		"eslint:recommended",
	],
	"globals": {
		"Atomics": "readonly",
		"SharedArrayBuffer": "readonly"
	},
	"parserOptions": {
		"ecmaVersion": 2018
	},
};