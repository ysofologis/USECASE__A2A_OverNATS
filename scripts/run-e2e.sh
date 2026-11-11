#!/usr/bin/env bash
# Run the binding package's e2e test suite against the live broker.
set -euo pipefail
cd /storage/drive-S/Work/SideProjects/A2A_OverNATS/packages/a2a-nats-binding
export A2A_E2E=1
exec node --test dist/test/e2e.test.js
