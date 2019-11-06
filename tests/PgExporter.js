const { expect } = require('chai');
const { Pool } = require('pg');
const sinon = require('sinon');
const pgExporter = require('../lib/exporter');

const sandbox = sinon.sandbox.create();

describe('PgExporter', () => {
  beforeEach(() => {

  });
  afterEach(() => {
    sandbox.restore();
  });

  it('exports a table with one geom column', async () => {
    const pgPool = new Pool();
    const pgQueryStub = sandbox.stub(pgPool, 'query');

    const exporter = pgExporter({
      variableUses: { LATITUDE: 'lat', LONGITUDE: 'lon' },
      createTable: true,
      schema: 'public',
      table: 'test',
      pgPool,
    });

    await exporter.init([
      {
        fieldName: 'lat',
        fieldType: 'float',
      },
      {
        fieldName: 'lon',
        fieldType: 'float',
      },
    ], 1);

    exporter.write([5, 5]);

    await exporter.finishWriting();

    expect(pgQueryStub.callCount).to.equal(2);

    expect(pgQueryStub.firstCall.args).to.deep.equal(['create table "public"."test"( geom geometry(point, 4326) );']);
    expect(pgQueryStub.secondCall.args).to.deep.equal(
      ['insert into "public"."test" values  ( \'srid=4326;point(5 5)\')', []],
    );
  });
});
