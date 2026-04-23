import { existsSync, readFileSync } from "node:fs";
import { resolve } from "node:path";

const repoRoot = process.cwd();
const requiredFiles = [resolve(repoRoot, "docs/evidence.example.json")];
const optionalFiles = [resolve(repoRoot, "artifacts/evidence/latest.json")];

function assert(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

function isObject(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function requireString(object, property, context) {
  assert(typeof object[property] === "string", `${context}.${property} must be a string`);
}

function requireNumber(object, property, context) {
  assert(
    typeof object[property] === "number" && Number.isFinite(object[property]),
    `${context}.${property} must be a finite number`,
  );
}

function validateMetric(metric, context) {
  assert(isObject(metric), `${context} must be an object`);
  requireString(metric, "label", context);
  requireString(metric, "value", context);
  if ("unit" in metric) {
    requireString(metric, "unit", context);
  }
  if ("tone" in metric) {
    requireString(metric, "tone", context);
  }
}

function validateGate(gate, context) {
  assert(isObject(gate), `${context} must be an object`);
  requireString(gate, "name", context);
  requireString(gate, "status", context);
  requireNumber(gate, "target", context);
  requireNumber(gate, "observed", context);
  if ("unit" in gate) {
    requireString(gate, "unit", context);
  }
}

function validateBenchmark(benchmark, context) {
  assert(isObject(benchmark), `${context} must be an object`);
  requireString(benchmark, "artifact", context);
  requireString(benchmark, "started_at_utc", context);
  requireString(benchmark, "completed_at_utc", context);
  requireNumber(benchmark, "target_eps", context);
  requireNumber(benchmark, "accepted_eps", context);
  requireNumber(benchmark, "processed_eps", context);
  requireNumber(benchmark, "query_p95_ms", context);
  requireNumber(benchmark, "peak_lag", context);
  requireNumber(benchmark, "post_load_drain_seconds", context);
  requireNumber(benchmark, "producer_count", context);
  requireNumber(benchmark, "processor_replicas", context);
  requireNumber(benchmark, "batch_size", context);
  requireString(benchmark, "summary", context);
  assert(Array.isArray(benchmark.gaps), `${context}.gaps must be an array`);
  benchmark.gaps.forEach((gap, index) => {
    assert(typeof gap === "string", `${context}.gaps[${index}] must be a string`);
  });
  assert(Array.isArray(benchmark.gates), `${context}.gates must be an array`);
  benchmark.gates.forEach((gate, index) => validateGate(gate, `${context}.gates[${index}]`));
}

function validateDrill(drill, context) {
  assert(isObject(drill), `${context} must be an object`);
  requireString(drill, "scenario_id", context);
  requireString(drill, "title", context);
  requireString(drill, "status", context);
  requireString(drill, "artifact", context);
  requireString(drill, "started_at_utc", context);
  requireString(drill, "completed_at_utc", context);
  requireString(drill, "result", context);
  requireString(drill, "operator_note", context);
  requireString(drill, "remaining_gap", context);
  assert(Array.isArray(drill.metrics), `${context}.metrics must be an array`);
  drill.metrics.forEach((metric, index) => validateMetric(metric, `${context}.metrics[${index}]`));
}

function validateEvidenceSummary(summary, filePath) {
  assert(isObject(summary), `${filePath} must contain a JSON object`);
  assert(summary.schema_version === 1, `${filePath}.schema_version must be 1`);
  requireString(summary, "generated_at", filePath);
  requireString(summary, "status", filePath);
  requireString(summary, "artifact_root", filePath);
  if (summary.benchmark !== null && summary.benchmark !== undefined) {
    validateBenchmark(summary.benchmark, `${filePath}.benchmark`);
  }
  assert(Array.isArray(summary.failure_drills), `${filePath}.failure_drills must be an array`);
  summary.failure_drills.forEach((drill, index) => {
    validateDrill(drill, `${filePath}.failure_drills[${index}]`);
  });
}

function validateFile(filePath) {
  const payload = JSON.parse(readFileSync(filePath, "utf8").replace(/^\uFEFF/, ""));
  validateEvidenceSummary(payload, filePath);
  console.log(`Validated evidence schema: ${filePath}`);
}

for (const filePath of requiredFiles) {
  validateFile(filePath);
}

for (const filePath of optionalFiles) {
  if (existsSync(filePath)) {
    validateFile(filePath);
  }
}
