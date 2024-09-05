const A = require('async')
const { createPromiseClient } = require('@connectrpc/connect-node')
const { Code } = require('@connectrpc/connect')
const { proto3, FileDescriptorSet } = require('@bufbuild/protobuf')
const debug = require('debug')('engine:grpc')

const { log: logger } = console

function ArtilleryGRPCEngine(script, ee, helpers) {
  this.script = script
  this.ee = ee
  this.helpers = helpers

  const { config } = this.script
  debug('script.config=%O', config)

  this.serviceClient = this.loadServiceClient()
  const client = this.initGRPCClient(config.target)
  this.clientHash = {
    [config.target]: client,
  }

  return this
}

ArtilleryGRPCEngine.prototype.loadServiceClient = function () {
  const {
    protobufDefinition,
  } = this.getEngineConfig()

  debug('protobufDefinition=%O', protobufDefinition)

  const {
    filepath,
    service,
  } = protobufDefinition

  // Use proto3 to load the .proto file and get the service definition
  const root = proto3.loadSync(filepath)
  const ServiceClient = root.lookupService(service)

  return ServiceClient
}

/**
 * Load test YAML configuration defined at: <config.engines.grpc>
 */
ArtilleryGRPCEngine.prototype.getEngineConfig = function () {
  const {
    channelOpts,
    protobufDefinition,
    metadata,
  } = this.script.config.engines.grpc

  return {
    channelOpts,
    protobufDefinition,
    metadata,
  }
}

ArtilleryGRPCEngine.prototype.initGRPCClient = function (target) {
  const { channelOpts } = this.getEngineConfig()

  // Connect RPC does not use channel options in the same way, but options can be passed
  const options = {
    http2: channelOpts.http2 || false, // Example: adjust according to your config
  }

  // Initialize the Connect RPC client using createPromiseClient
  const client = createPromiseClient(this.serviceClient, target, options)
  return client
}

ArtilleryGRPCEngine.prototype.createScenario = function (scenarioSpec, ee) {
  const tasks = scenarioSpec.flow.map((ops) => this.step(ops, ee, scenarioSpec))

  return this.compile(tasks, scenarioSpec.flow, ee)
}

ArtilleryGRPCEngine.prototype.buildGRPCMetadata = function () {
  const { metadata } = this.getEngineConfig()
  const headers = {}

  Object.entries(metadata).forEach(([k, v]) => {
    headers[k] = v
  })

  return headers
}

ArtilleryGRPCEngine.prototype.step = function step(ops, ee, scenarioSpec) {
  if (ops.log) {
    return (context, callback) => {
      logger(this.helpers.template(ops.log, context))
      return process.nextTick(() => { callback(null, context) })
    }
  }

  const startedAt = process.hrtime()

  function recordMetrics(startedAt, error, response) {
    ee.emit('counter', 'engine.grpc.responses.total', 1)
    if (error) {
      ee.emit('counter', 'engine.grpc.responses.error', 1)
      ee.emit('counter', 'engine.grpc.codes.' + error.code, 1)
    } else {
      ee.emit('counter', 'engine.grpc.responses.success', 1)
      ee.emit('counter', 'engine.grpc.codes.' + Code.OK, 1)
    }

    const [diffSec, diffNanosec] = process.hrtime(startedAt)
    const deltaNanosec = (diffSec * 1e9) + diffNanosec
    const deltaMillisec = deltaNanosec / 1e6
    ee.emit('histogram', 'engine.grpc.response_time', deltaMillisec)
  }

  function beforeRequestHook(context, config, scenarioSpec) {
    if (!scenarioSpec.beforeRequest) { return }

    Array.from(scenarioSpec.beforeRequest).forEach((functionName) => {
      const f = config.processor[functionName]
      if (f) { f(null, context, null, () => {}) }
    })
  }

  return (context, callback) => {
    beforeRequestHook(context, this.script.config, scenarioSpec)

    const target = context.vars.target
    let client = this.clientHash[target]
    if (!client) {
      client = this.initGRPCClient(target)
      this.clientHash[target] = client
    }
    const headers = this.buildGRPCMetadata()

    Object.keys(ops).map((rpcName) => {
      const args = this.helpers.template(ops[rpcName], context)

      // Make a Connect RPC request
      client[rpcName](args, { headers })
        .then(response => {
          recordMetrics(startedAt, null, response)
          callback(null, context)
        })
        .catch(error => {
          recordMetrics(startedAt, error, null)
          ee.emit('error', error)
          callback(error, context)
        })
    })
  }
}

ArtilleryGRPCEngine.prototype.compile = function (tasks, scenarioSpec, ee) {
  return function scenario(initialContext, callback) {
    const init = function init(next) {
      ee.emit('started')
      return next(null, initialContext)
    }

    const steps = [init].concat(tasks)

    A.waterfall(
      steps,
      function done(err, context) {
        if (err) {
          debug(err)
        }

        return callback(err, context)
      }
    )
  }
}

module.exports = ArtilleryGRPCEngine
