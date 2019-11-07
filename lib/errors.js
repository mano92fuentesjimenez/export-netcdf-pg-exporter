/* eslint-disable max-classes-per-file */

class InitializationError extends Error {
  constructor() {
    super('Export was already initialized');
  }
}

class NoInitializatedError extends Error {
  constructor() {
    super('Exporter was not initialized');
  }
}


class BadGeomConfigurationError extends Error {
  constructor() {
    super('Configured variables to be used as a geometry but variables were not given');
  }
}

module.exports = {
  InitializationError,
  BadGeomConfigurationError,
  NoInitializatedError,
};
