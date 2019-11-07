const { Pool } = require('pg');
const getType = require('./utils/getType');
const {
  InitializationError,
  BadGeomConfigurationError,
  NoInitializatedError,
} = require('./errors');

class PgExporter {
  constructor({
    connection,
    schema = 'public',
    table,
    createTable = false,
    variableUses = {},
    pgPool,
  }) {
    this.rowsDataInserted = 0;
    this.rowsBatchMax = 200;
    this.initialized = false;
    this.srid = 4326;

    this.connection = connection;
    this.schema = schema;
    this.table = table;
    this.createTable = createTable;
    this.variableUses = variableUses;
    this.pgPool = pgPool;

    if (!this.pgPool) {
      this.pgPool = new Pool(this.connection);
    }
    this.rowsData = [];

    this.useGeom = this.variableUses.LATITUDE || this.variableUses.LONGITUDE;
  }

  init(variables, totalRows) {
    if (this.initialized) {
      return Promise.reject(new InitializationError());
    }
    this.initialized = true;

    this.totalRows = totalRows;

    let createTableStr = `create table "${this.schema}"."${this.table}"(`;
    if (this.useGeom) {
      createTableStr = `${createTableStr} geom geometry(point, ${this.srid})`;
    }
    if (this.variableUses.TIME) {
      createTableStr = `${createTableStr}${this.useGeom ? ',' : ''} time timestamp`;
    }

    let fields = '';
    variables.forEach((value, key) => {
      if (this.variableUses.LONGITUDE === value.fieldName) {
        this.longitudeVarIndex = key;
        return;
      }
      if (this.variableUses.LATITUDE === value.fieldName) {
        this.latitudeVarIndex = key;
        return;
      }
      if (this.variableUses.TIME === value.fieldName) {
        this.timeVarIndex = key;
        return;
      }
      if (Object.values(this.variableUses).includes(value.fieldName)) return;
      fields = `${fields}, ${value.fieldName} ${getType(value.fieldType)}`;
    });

    if (this.useGeom
      && (this.longitudeVarIndex === undefined || this.latitudeVarIndex === undefined)
    ) {
      return Promise.reject(new BadGeomConfigurationError());
    }

    if (!this.variableUses.TIME && !this.useGeom) {
      fields = fields.slice(1, fields.length);
    }

    createTableStr = `${createTableStr}${fields} );`;
    if (this.createTable) {
      return this.pgPool.query(createTableStr);
    }
    return Promise.resolve(true);
  }

  async write(row) {
    if (!this.initialized) {
      return Promise.reject(new NoInitializatedError());
    }

    this.rowsData.push(row);
    this.rowsDataInserted++;
    if (this.rowsData.length === this.rowsBatchMax || this.rowsDataInserted === this.totalRows) {
      await this.writeRowBatch();
      this.rowsData = [];
    }
    return Promise.resolve(true);
  }

  writeRowBatch() {
    let insertStr = `insert into "${this.schema}"."${this.table}" values `;
    const values = [];

    const specialValuesIndexes = {};
    if (this.useGeom) {
      specialValuesIndexes[this.latitudeVarIndex] = true;
      specialValuesIndexes[this.longitudeVarIndex] = true;
    }
    if (this.variableUses.TIME) {
      specialValuesIndexes[this.timeVarIndex] = true;
    }
    if (!this.useGeom && !this.variableUses.TIME) {
      specialValuesIndexes[0] = true;
    }

    this.rowsData.forEach((rowData, rowNumber) => {
      if (rowNumber > 0) {
        insertStr = `${insertStr}, `;
      }
      if (this.useGeom) {
        insertStr = `${insertStr}('srid=${this.srid};point(${rowData[this.latitudeVarIndex]} ${rowData[this.longitudeVarIndex]})'`;
      }
      if (this.variableUses.TIME) {
        insertStr = `${insertStr}${this.useGeom ? ', ' : '('}$${values.length + 1}`;
        values.push(new Date(rowData[this.timeVarIndex]));
      }
      if (!this.variableUses.TIME && !this.useGeom) {
        insertStr = `${insertStr}($${values.length + 1}`;
        values.push(rowData[0]);
      }

      rowData.forEach((value, valueIndex) => {
        if (specialValuesIndexes[valueIndex]) {
          return;
        }
        insertStr = `${insertStr}, $${values.length + 1}`;
        values.push(value);
      });
      insertStr = `${insertStr})`;
    });

    return this.pgPool.query(insertStr, values);
  }

  finishWriting() {
    return this.pgPool.end();
  }
}

module.exports = PgExporter;
