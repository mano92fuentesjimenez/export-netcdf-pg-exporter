const getType = require('./utils/getType');
const _ = require('lodash');

const { Pool } = require('pg');

class PgExporter {

  pgPool;
  schema;
  table;
  totalRows;
  createTable;

  rowsData;
  rowsDataInserted = 0;
  rowsBatchMax = 200;

  constructor(connection, schema, table, createTable) {
    this.pgPool = new Pool(connection);
    this.schema = schema;
    this.table = table;
    this.createTable = createTable;
    this.rowsData = [];
  }

  init(variables, totalRows) {
    this.totalRows = totalRows;

    if(this.createTable){

      let createTableStr = `create table "${this.schema}"."${this.table}"(`;
      if(variables.LONGITUDE && variables.LATITUDE) {
        createTableStr = `${createTableStr} 'geom geometry(point, 4326)`;
      }
      if(variables.TIME) {
        createTableStr = `${createTableStr}, time timestampz`;
      }
      _.each(variables, (value, key) => {
        if(['TIMES','LONGITUDE','LATITUDE'].includes(key))
          return;
        createTableStr = `${createTableStr}, ${key} ${getType(value)}`;
      });

      createTableStr = `${createTableStr} );`;
      return this.pgPool.query(createTableStr);
    }
  }

  write(row) {
    this.rowsData.push(row);
    this.rowsDataInserted++;
    if(this.rowsData.length === this.rowsBatchMax || this.rowsDataInserted === this.totalRows) {
      this.writeRowBatch();
      this.rowsData = [];
    }
    return Promise.resolve(true);
  }

  writeRowBatch() {

    let insertStr = `insert into "${this.schema}"."${this.table}" values `;
    const values = [];

    this.rowsData.forEach( rowData => {
      const entries = Object.entries(rowData);
      const [firstValue, ...data ] = entries;

      insertStr = `${insertStr} ($${values.length}`;
      values.push(firstValue);

      data.forEach(value => {
        insertStr = `${insertStr}, $${values.length}`;
        values.push(value);
      });
      insertStr = `${insertStr})`;
    });

    this.pgPool.query(insertStr, values);
  }

  finishWriting(){
    return this.pgPool.end();
  }

}

module.exports = function (
  {
    connection,
    schema,
    table,
    createTable = false,
  }) {
  return new PgExporter(connection,schema,table,createTable);
};
