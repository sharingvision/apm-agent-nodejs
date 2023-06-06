/*
 * Copyright Elasticsearch B.V. and other contributors where applicable.
 * Licensed under the BSD 2-Clause License; you may not use this file except in
 * compliance with the BSD 2-Clause License.
 */

'use strict'

const test = require('tape')

const { CONFIG_SCHEMA } = require('../../lib/config/schema-new')
const Agent = require('../../lib/agent')
const { NoopApmClient } = require('../../lib/apm-client/noop-apm-client')
const { createMockLogger } = require('../_mock_logger')

// Options to pass to `agent.start()` to turn off some default agent behavior
// that is unhelpful for these tests.
const agentOpts = {
  centralConfig: false,
  captureExceptions: false,
  metricsInterval: '0s',
  cloudProvider: 'none',
  logLevel: 'warn'
}
const logCalls = []
const agentOptsNoTransportNoLogger = Object.assign(
  {},
  agentOpts,
  {
    logger: createMockLogger(logCalls),
    transport: function createNoopApmClient () {
      // Avoid accidentally trying to send data to an APM server.
      return new NoopApmClient()
    }
  }
)

/**
 * Tells if a config option definition has the given normalizer for a type
 *
 * @param {Object} def The definition of the config option
 * @param {Array<string> | undefined} def.normalizers List of the normalizers for that option
 * @param {string} type one of Bool | Number | KeyValuePair | Infinity | Duration
 * @returns {Boolean}
 */
function hasType (def, type) {
  // return def.types && def.types.indexOf(type) !== -1
  // if (def.normalizers) {
  //   def.normalizers.forEach(f => console.log(f.name))
  // }
  return def.normalizers && def.normalizers.some((fn) => fn.name.endsWith(type))
}

/**
 * Asserts that ENV vars set the option values and overrides the ones used in `agent.start()`
 *
 * @param {Object} t Test object instance
 * @param {Object} optionDef The definition of the config option
 * @param {*} value1 If value2 param defined { the value from `agent.start` options } else { the value from ENV }
 * @param {*} value2 If defined it holde the value from ENV
 */
function assertEnvVar (t, optionDef, value1, value2) {
  const existingEnvValue = process.env[optionDef.envVar]
  const agent = new Agent()
  const opts = {}
  let value = value1

  if (value2) {
    opts[optionDef.name] = value1
    value = value2
  }

  process.env[optionDef.envVar] = value.toString()
  agent.start(agentOptsNoTransportNoLogger)
  t.strictEqual(agent._conf[optionDef.name], value)

  process.env[optionDef.envVar] = existingEnvValue
  agent.destroy()
}

// Test BOOLS that can be set via ENV
const BOOL_OPTS_WITH_ENV_EXCLUDED = [
  // This ones depend on other options so we should have specific tests
  'captureSpanStackTraces',
  'asyncHooks',
  // this one seems to have a bug
  'breakdownMetrics'
]
const BOOL_OPTS_WITH_ENV = CONFIG_SCHEMA.filter(function (def) {
  const isExcluded = BOOL_OPTS_WITH_ENV_EXCLUDED.indexOf(def.name) !== -1
  return ('envVar' in def) && !isExcluded && hasType(def, 'Bool')
})

BOOL_OPTS_WITH_ENV.forEach(function (option) {
  test(`${option.name} should be configurable by environment variable ${option.envVar}`, function (t) {
    assertEnvVar(t, option, !option.defaultValue)
    t.end()
  })

  test(`should overwrite option property ${option.name} by environment variable ${option.envVar}`, function (t) {
    assertEnvVar(t, option, !option.defaultValue, !!option.defaultValue)
    t.end()
  })
})

// Test NUMS that can be set via ENV
const NUM_OPTS_WITH_ENV = CONFIG_SCHEMA.filter(function (def) {
  return ('envVar' in def) && hasType(def, 'Number')
})

NUM_OPTS_WITH_ENV.forEach(function (option) {
  test(`${option.name} should be configurable by environment variable ${option.envVar}`, function (t) {
    assertEnvVar(t, option, 1)
    t.end()
  })

  test(`should overwrite option property ${option.name} by environment variable ${option.envVar}`, function (t) {
    // 'transactionSampleRate' only accepts values between [0,1]
    if (option.name === 'transactionSampleRate') {
      assertEnvVar(t, option, 1, 0.5)
    } else {
      assertEnvVar(t, option, 1, 2)
    }
    t.end()
  })
})

// TODO: skip for now
test.skip('configuration options coverage', function (t) {
  const tested = new Set([].concat(
    BOOL_OPTS_WITH_ENV,
    NUM_OPTS_WITH_ENV
  ))

  CONFIG_SCHEMA.forEach(function (def) {
    t.ok(tested.has(def), `config option ${def.name} is tested`)
  })
  t.end()
})
