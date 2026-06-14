// OPTIONAL — for PLAIN Fastify services (no NestJS).
//
// This is the auto-instrumentation entry the real crowncoins missions service uses:
// the standard Node auto-instrumentations PLUS @fastify/otel registered with
// registerOnInitialization, which instruments the Fastify framework and names the
// HTTP server span by route. On a plain Fastify app this "just works" — no app code,
// no span suppression.
//
// Build it into an OTel Operator init image (see Dockerfile.autoinstrumentation) and
// point the Instrumentation CR's spec.nodejs.image at it.
//
// NOTE: this lab runs NestJS-on-Fastify, where @fastify/otel + the Nest adapter
// conflict (duplicate/suppressed server spans). The lab therefore uses the STOCK
// init image + an app-level route-naming hook (src/main.ts) instead of this file.
// Use THIS file only if your service is plain Fastify.
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

// getNodeAutoInstrumentations() honors OTEL_NODE_DISABLED_INSTRUMENTATIONS.
const sdkConfig = {
  instrumentations: [
    ...getNodeAutoInstrumentations(),
    new FastifyOtelInstrumentation({ registerOnInitialization: true }),
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
