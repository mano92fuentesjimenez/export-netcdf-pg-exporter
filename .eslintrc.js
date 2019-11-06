module.exports = {
	"env": {
		"commonjs": true,
		"es6": true,
		"node": true,
		"mocha": true,
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
	rules: {
		"no-plusplus": 0,
	}
};
