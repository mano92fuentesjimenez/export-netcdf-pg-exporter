const { Pool } = require('pg');
const getType = require('./utils/getType');

class PgExporter {
  constructor(conf) {
    this.rowsDataInserted = 0;
    this.rowsBatchMax = 200;
    this.variableUses = {};
    this.initialized = false;

    Object.assign(this, conf);

    this.pgPool = new Pool(conf.connection);
    this.rowsData = [];

    this.useGeom = this.variableUses.LATITUDE && this.variableUses.LONGITUDE;

    if (!this.useGeom) {
      delete this.variableUses.LATITUDE;
      delete this.variableUses.LONGITUDE;
    }
  }

  init(variables, totalRows) {
    if (this.initialized) {
      throw new Error('Exporta was initialized');
    }
    this.initialized = true;

    this.totalRows = totalRows;

    let createTableStr = `create table "${this.schema}"."${this.table}"(`;
    if (this.useGeom) {
      createTableStr = `${createTableStr} 'geom geometry(point, 4326)`;
    }
    if (variables.TIME) {
      createTableStr = `${createTableStr}, time timestampz`;
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
        this.longitudeVarIndex = key;
        return;
      }
      if (Object.values(this.variableUses).includes(value.fieldName)) return;
      fields = `${fields}, ${value.fieldName} ${getType(value.fieldType)}`;
    });

    createTableStr = `${createTableStr} );`;
    if (this.createTable) {
      return this.pgPool.query(createTableStr);
    }
    return Promise.resolve(true);
  }

  write(row) {
    if (!this.initialized) {
      throw new Error('Exporter was not initialized');
    }

    this.rowsData.push(row);
    this.rowsDataInserted++;
    if (this.rowsData.length === this.rowsBatchMax || this.rowsDataInserted === this.totalRows) {
      this.writeRowBatch();
      this.rowsData = [];
    }
    return Promise.resolve(true);
  }

  writeRowBatch() {
    let insertStr = `insert into "${this.schema}"."${this.table}" values `;
    const values = [];

    this.rowsData.forEach((rowData) => {
      if (this.useGeom) {
        insertStr = `${insertStr} ( 'point(${rowData[this.latitudeVarIndex]}, ${rowData[this.longitudeVarIndex]})`;
        rowData.splice(Math.max(this.latitudeVarIndex, this.longitudeVarIndex), 1);
        rowData.splice(Math.min(this.latitudeVarIndex, this.longitudeVarIndex), 1);
      }
      if (this.variableUses.TIME) {
        insertStr = `${insertStr} ${this.useGeom ? ',' : ''} $${values.length}`;
        values.push(new Date(rowData[this.timeVarIndex]));
      }
      if (!this.variableUses.TIME && !this.useGeom) {
        insertStr = `${insertStr} ($${values.length}`;
        values.push(rowData.shift());
      }

      rowData.forEach((value) => {
        insertStr = `${insertStr}, $${values.length}`;
        values.push(value);
      });
      insertStr = `${insertStr})`;
    });

    this.pgPool.query(insertStr, values);
  }

  finishWriting() {
    return this.pgPool.end();
  }
}

module.exports = function (
  {
    connection,
    schema,
    table,
    createTable = false,
    variableUses,
  },
) {
  return new PgExporter({
    connection, schema, table, createTable, variableUses,
  });
};
