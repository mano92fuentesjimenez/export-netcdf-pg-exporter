const { expect } = require('chai');
const { Pool } = require('pg');
const sinon = require('sinon');
const pgExporter = require('../lib/exporter');

const sandbox = sinon.sandbox.create();

describe('PgExporter', () => {
  let pgPool;
  let pgQueryStub;
  let pgEndStub;

  beforeEach(() => {
    pgPool = new Pool();
    pgQueryStub = sandbox.stub(pgPool, 'query');
    pgEndStub = sandbox.stub(pgPool, 'end');
  });

  afterEach(async () => {
    sandbox.restore();

    if (!pgPool.ended) {
      await pgPool.end();
    }
  });

  it('exports a table with one geom column', async () => {
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
    expect(pgEndStub.callCount).to.equal(1);
  });

  it('exports a table with two columns one geom column and another float column', async () => {
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
      {
        fieldName: 'number',
        fieldType: 'float',
      },
    ], 1);

    exporter.write([5, 5, 20]);

    await exporter.finishWriting();

    expect(pgQueryStub.callCount).to.equal(2);

    expect(pgQueryStub.firstCall.args).to.deep.equal(['create table "public"."test"( geom geometry(point, 4326), number numeric );']);
    expect(pgQueryStub.secondCall.args).to.deep.equal(
      ['insert into "public"."test" values  ( \'srid=4326;point(5 5)\', $0)', [20]],
    );
    expect(pgEndStub.callCount).to.equal(1);
  });
});
