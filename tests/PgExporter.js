const { Pool } = require('pg');
const sinon = require('sinon');
const { expect } = require('./setup');
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
    ], 2);

    await exporter.write([5, 5]);
    await exporter.write([10, 50]);

    await exporter.finishWriting();

    expect(pgQueryStub.callCount).to.equal(2);

    expect(pgQueryStub.firstCall.args).to.deep.equal(['create table "public"."test"( geom geometry(point, 4326) );']);
    expect(pgQueryStub.secondCall.args).to.deep.equal(
      ['insert into "public"."test" values (\'srid=4326;point(5 5)\'), (\'srid=4326;point(10 50)\')', []],
    );
    expect(pgEndStub.callCount).to.equal(1);
  });

  it('exports a table with a Times column', async () => {
    const exporter = new PgExporter({
      variableUses: { TIME: 'time' },
      createTable: true,
      schema: 'public',
      table: 'test',
      pgPool,
    });

    await exporter.init([
      {
        fieldName: 'time',
        fieldType: 'char',
      },
    ], 2);

    const date = '1995-12-17T03:24:00';
    await exporter.write([date]);
    await exporter.write([date]);

    await exporter.finishWriting();

    expect(pgQueryStub.callCount).to.equal(2);

    expect(pgQueryStub.firstCall.args).to.deep.equal(['create table "public"."test"( time timestamp );']);
    expect(pgQueryStub.secondCall.args).to.deep.equal(
      ['insert into "public"."test" values ($1), ($2)', [new Date(date), new Date(date)]],
    );
    expect(pgEndStub.callCount).to.equal(1);
  });

  it('exports a table with a Times column and a geom coulumn', async () => {
    const exporter = new PgExporter({
      variableUses: {
        TIME: 'time',
        LATITUDE: 'lat',
        LONGITUDE: 'lon',
      },
      createTable: true,
      schema: 'public',
      table: 'test',
      pgPool,
    });

    await exporter.init([
      {
        fieldName: 'time',
        fieldType: 'char',
      },
      {
        fieldName: 'lat',
        fieldType: 'float',
      },
      {
        fieldName: 'lon',
        fieldType: 'float',
      },
    ], 2);

    const date = '1995-12-17T03:24:00';
    await exporter.write([date, 4, 3]);
    await exporter.write([date, 8, 10]);

    await exporter.finishWriting();

    expect(pgQueryStub.callCount).to.equal(2);

    expect(pgQueryStub.firstCall.args).to.deep.equal(['create table "public"."test"( geom geometry(point, 4326), time timestamp );']);
    expect(pgQueryStub.secondCall.args).to.deep.equal(
      ['insert into "public"."test" values (\'srid=4326;point(4 3)\', $1), (\'srid=4326;point(8 10)\', $2)', [new Date(date), new Date(date)]],
    );
    expect(pgEndStub.callCount).to.equal(1);
  });

  it('exports a table with a Times column and a number coulumn', async () => {
    const exporter = new PgExporter({
      variableUses: {
        TIME: 'time',
      },
      createTable: true,
      schema: 'public',
      table: 'test',
      pgPool,
    });

    await exporter.init([
      {
        fieldName: 'time',
        fieldType: 'char',
      },
      {
        fieldName: 'lon',
        fieldType: 'float',
      },
    ], 2);

    const date = '1995-12-17T03:24:00';
    await exporter.write([date, 4]);
    await exporter.write([date, 9]);

    await exporter.finishWriting();

    expect(pgQueryStub.callCount).to.equal(2);

    expect(pgQueryStub.firstCall.args).to.deep.equal(['create table "public"."test"( time timestamp, lon numeric );']);
    expect(pgQueryStub.secondCall.args).to.deep.equal(
      ['insert into "public"."test" values ($1, $2), ($3, $4)', [new Date(date), 4, new Date(date), 9]],
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
    ], 2);

    await exporter.write([5, 5, 20]);
    await exporter.write([10, 15, 200]);

    await exporter.finishWriting();

    expect(pgQueryStub.callCount).to.equal(2);

    expect(pgQueryStub.firstCall.args).to.deep.equal(['create table "public"."test"( geom geometry(point, 4326), number numeric );']);
    expect(pgQueryStub.secondCall.args).to.deep.equal(
      ['insert into "public"."test" values (\'srid=4326;point(5 5)\', $1), (\'srid=4326;point(10 15)\', $2)', [20, 200]],
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
    ], 2);

    await exporter.write([5]);
    await exporter.write([22]);

    await exporter.finishWriting();

    expect(pgQueryStub.callCount).to.equal(2);

    expect(pgQueryStub.firstCall.args).to.deep.equal(['create table "public"."test"( number numeric );']);
    expect(pgQueryStub.secondCall.args).to.deep.equal(
      ['insert into "public"."test" values ($1), ($2)', [5, 22]],
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

    const result = exporter.init([
      {
        fieldName: 'number',
        fieldType: 'float',
      },
    ], 1);

    expect(result).to.eventually.throw(BadGeomConfigurationError);
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

    const result = exporter.init([
      {
        fieldName: 'number',
        fieldType: 'float',
      },
    ], 1);

    expect(result).to.eventually.throw(BadGeomConfigurationError);
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

    const result = exporter.init([
      {
        fieldName: 'number',
        fieldType: 'float',
      },
    ], 1);

    expect(result).to.eventually.throw(InitializationError);
  });

  it('fails because exporter was not initialized', async () => {
    const exporter = new PgExporter({
      createTable: true,
      schema: 'public',
      table: 'test',
      pgPool,
    });

    const result = exporter.write([5, 5]);

    expect(result).to.eventually.throw(NoInitializatedError);
  });
});
