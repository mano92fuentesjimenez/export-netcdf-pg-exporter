const { expect } = require('chai');
const { Pool } = require('pg');
const sinon = require('sinon');
const PgExporter = require('../lib/exporter');
const {
  InitializationError,
  BadGeomConfigurationError,
  NoInitializatedError,
} = require('../lib/errors');

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
    const exporter = new PgExporter({
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
    const exporter = new PgExporter({
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

  it('exports a table with one number column', async () => {
    const exporter = new PgExporter({
      createTable: true,
      schema: 'public',
      table: 'test',
      pgPool,
    });

    await exporter.init([
      {
        fieldName: 'number',
        fieldType: 'float',
      },
    ], 1);

    exporter.write([5]);

    await exporter.finishWriting();

    expect(pgQueryStub.callCount).to.equal(2);

    expect(pgQueryStub.firstCall.args).to.deep.equal(['create table "public"."test"( number numeric );']);
    expect(pgQueryStub.secondCall.args).to.deep.equal(
      ['insert into "public"."test" values  ($0)', [5]],
    );
    expect(pgEndStub.callCount).to.equal(1);
  });

  it('fails because a geometry use was partially defined with latitude', async () => {
    const exporter = new PgExporter({
      variableUses: {
        LATITUDE: 'number',
      },
      createTable: true,
      schema: 'public',
      table: 'test',
      pgPool,
    });

    await exporter.init([
      {
        fieldName: 'number',
        fieldType: 'float',
      },
    ], 1)
      .catch(
        (error) => expect(error).to.be.instanceOf(BadGeomConfigurationError),
      );
  });

  it('fails because a geometry use was partially defined with longitude', async () => {
    const exporter = new PgExporter({
      variableUses: {
        LONGITUDE: 'number',
      },
      createTable: true,
      schema: 'public',
      table: 'test',
      pgPool,
    });

    await exporter.init([
      {
        fieldName: 'number',
        fieldType: 'float',
      },
    ], 1)
      .catch(
        (error) => expect(error).to.be.instanceOf(BadGeomConfigurationError),
      );
  });

  it('fails because exporter was initialized twice', async () => {
    const exporter = new PgExporter({
      createTable: true,
      schema: 'public',
      table: 'test',
      pgPool,
    });

    await exporter.init([
      {
        fieldName: 'number',
        fieldType: 'float',
      },
    ], 1);

    await exporter.init([
      {
        fieldName: 'number',
        fieldType: 'float',
      },
    ], 1)
      .catch(
        (error) => expect(error).to.be.instanceOf(InitializationError),
      );
  });

  it('fails because exporter was not initialized', async () => {
    const exporter = new PgExporter({
      createTable: true,
      schema: 'public',
      table: 'test',
      pgPool,
    });

    exporter.write([5, 5])
      .catch(
        (error) => expect(error).to.be.instanceOf(NoInitializatedError),
      );
  });
});
