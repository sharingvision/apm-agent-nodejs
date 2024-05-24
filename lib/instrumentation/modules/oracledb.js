/*
 * Copyright Elasticsearch B.V. and other contributors where applicable.
 * Licensed under the BSD 2-Clause License; you may not use this file except in
 * compliance with the BSD 2-Clause License.
 */

'use strict';

const EventEmitter = require('events');

var semver = require('semver');
var sqlSummary = require('sql-summary');

var shimmer = require('../shimmer');
var symbols = require('../../symbols');
var { getDBDestination } = require('../context');

module.exports = function (oracledb, agent, { version, enabled }) {
  if (!enabled) {
    return oracledb;
  }
  if (!semver.satisfies(version, '>=6.5.0 <7.0.0')) {
    agent.logger.debug('oracledb version %s not supported - aborting...', version);
    return oracledb;
  }

  patchClient(oracledb.Connection, 'oracledb.Connection', agent);

  return oracledb;
};

function rsplit(s, sep, maxsplit) {
  var split = s.split(sep);
  return maxsplit ? [ split.slice(0, -maxsplit).join(sep) ].concat(split.slice(-maxsplit)) : split;
}

function patchClient(Client, klass, agent) {
  agent.logger.debug('shimming %s.prototype.execute', klass);
  shimmer.wrap(Client.prototype, 'execute', wrapQuery);

  function wrapQuery(orig, name) {
    return function wrappedFunction(sql) {
      agent.logger.debug('intercepted call to %s.prototype.%s', klass, name);
      const ins = agent._instrumentation;
      const span = ins.createSpan('SQL', 'db', 'oracledb', 'query', {
        exitSpan: true,
      });
      if (!span) {
        return orig.apply(this, arguments);
      }

      // Get connection parameters from Connection.
      let host, port, database, sid, instance, service;
      if (typeof this._impl === 'object') {
        [host, port] = rsplit(this._impl.remoteAddress, ':', 1);
        database = this._impl.getDbName();
        sid = this._impl.sid;
        instance = this._impl.getInstanceName();
        service = this._impl.getServiceName();
      }
      span._setDestinationContext(getDBDestination(host, port));

      const dbContext = { type: 'sql' };
      let sqlText = sql;
      if (typeof sqlText === 'string') {
        span.name = sqlSummary(sqlText);
        dbContext.statement = sqlText;
      } else {
        agent.logger.debug(
          'unable to parse sql form oracledb module (type: %s)',
          typeof sqlText,
        );
      }
      if (database || instance || service) {
        dbContext.instance = `${service || sid}@${instance || database}`;
      }
      span.setDbContext(dbContext);

      if (this[symbols.knexStackObj]) {
        span.customStackTrace(this[symbols.knexStackObj]);
        this[symbols.knexStackObj] = null;
      }

      let index = arguments.length - 1;
      let cb = arguments[index];
      if (Array.isArray(cb)) {
        index = cb.length - 1;
        cb = cb[index];
      }

      const spanRunContext = ins.currRunContext().enterSpan(span);
      const onQueryEnd = ins.bindFunctionToRunContext(
        spanRunContext,
        (_err) => {
          agent.logger.debug('intercepted end of %s.prototype.%s', klass, name);
          span.end();
        },
      );

      if (typeof cb === 'function') {
        arguments[index] = ins.bindFunction((err, res) => {
          onQueryEnd(err);
          return cb(err, res);
        });
        return orig.apply(this, arguments);
      } else {
        var queryOrPromise = orig.apply(this, arguments);

        if (typeof queryOrPromise.on === 'function') {
          queryOrPromise.on('end', onQueryEnd);
          queryOrPromise.on('error', onQueryEnd);
          if (queryOrPromise instanceof EventEmitter) {
            ins.bindEmitter(queryOrPromise);
          }
        } else if (typeof queryOrPromise.then === 'function') {
          queryOrPromise.then(() => {
            onQueryEnd();
          }, onQueryEnd);
        } else {
          agent.logger.debug(
            'ERROR: unknown oracledb query type: %s',
            typeof queryOrPromise,
          );
        }

        return queryOrPromise;
      }
    };
  }
}
