//go:build server

package main

// Build-time gate, deliberately OFF. Do not enable before cold-filter behavior,
// source clock provenance, model runtime and physical acceptance are resolved.
const pairedInputExperimental = false
