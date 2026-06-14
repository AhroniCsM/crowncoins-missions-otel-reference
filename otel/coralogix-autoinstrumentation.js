// Custom Node.js auto-instrumentation entry used by the OpenTelemetry Operator
// init image. Identical in spirit to the real crowncoins-production-missions
// service: standard Node auto-instrumentations PLUS @fastify/otel registered with
// registerOnInitialization:true, which instruments the Fastify framework even when
// it is wrapped by the NestJS Fastify adapter (a plain app-level plugin cannot,
// because Nest seals the Fastify instance before app code runs).
const opentelemetry = require('@opentelemetry/sdk-node');
const { diag, DiagConsoleLogger } = require('@opentelemetry/api');
const { getStringFromEnv, diagLogLevelFromString } = require('@opentelemetry/core');
const {
  getNodeAutoInstrumentations,
  getResourceDetectorsFromEnv,
} = require('@opentelemetry/auto-instrumentations-node');
const FastifyOtelInstrumentation = require('@fastify/otel');

const logLevel = getStringFromEnv('OTEL_LOG_LEVEL');
if (logLevel != null) {
  diag.setLogger(new DiagConsoleLogger(), {
    logLevel: diagLogLevelFromString(logLevel),
  });
}

// getNodeAutoInstrumentations() honors OTEL_NODE_DISABLED_INSTRUMENTATIONS
// (e.g. "undici" to drop the load generator's outgoing client spans).
const sdkConfig = {
  instrumentations: [
    ...getNodeAutoInstrumentations({
      // Under the Nest Fastify adapter, @opentelemetry/instrumentation-http also
      // emits an incoming server span, duplicating @fastify/otel's. Suppress it so
      // @fastify/otel owns the single, route-named server span.
      '@opentelemetry/instrumentation-http': {
        ignoreIncomingRequestHook: () => true,
      },
    }),
    new FastifyOtelInstrumentation({
      registerOnInitialization: true,
      // Drop the noisy per-lifecycle-hook spans (onRequest - middie, etc.).
      instrumentHooks: false,
      // Name the server span exactly like the real service:
      // "Fastify/GET//missions/v2/get-user-missions".
      requestHook: (span, request) => {
        try {
          const route = request?.routeOptions?.url || request?.routerPath;
          const method = request?.method;
          if (route && method) {
            span.updateName(`Fastify/${method}/${route}`);
            span.setAttribute('http.route', route);
          }
        } catch (_e) {
          /* never break a request over instrumentation */
        }
      },
    }),
  ],
};
// getResourceDetectorsFromEnv is only exported by newer auto-instrumentations-node
// versions; guard so the script also runs on older operator base images.
if (typeof getResourceDetectorsFromEnv === 'function') {
  sdkConfig.resourceDetectors = getResourceDetectorsFromEnv();
}

const sdk = new opentelemetry.NodeSDK(sdkConfig);

try {
  sdk.start();
  diag.info('Coralogix OTel autoinstrumentation started (with @fastify/otel)');
} catch (error) {
  diag.error('Error initializing OpenTelemetry SDK', error);
}

const shutdown = async () => {
  try {
    await sdk.shutdown();
    diag.debug('OpenTelemetry SDK terminated');
  } catch (error) {
    diag.error('Error terminating OpenTelemetry SDK', error);
  }
};

process.on('SIGTERM', shutdown);
process.once('beforeExit', shutdown);
