import os
import time
import logging
from datetime import datetime, timezone

import requests
from flask import Flask, request, jsonify
import google.auth
from google.auth.transport.requests import Request as GoogleAuthRequest
from google.oauth2 import id_token as google_id_token

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# nightly-orchestrator
#
# Thin sequence-of-services orchestrator for the Early Alert nightly pipeline.
# Calls four existing Cloud Run services in dependency order, status-gating each
# before the next:
#
#   1. backup-textit-flows  (/backup)  — capture TextIt state BEFORE any writes
#   2. contacts-sync        (/sync)    — TextIt -> BQ users full sync (TextIt-wins)
#   3. state-vamc-sync      (/sync)    — set state + vamc_presumed (Sta#), BQ+TextIt
#   4. vamc-sync            (/sync)    — derive vamc_display_name from vamc_presumed
#
# Ordering rationale (data dependencies, NOT preference):
#   - backup first: snapshot before mutations.
#   - contacts-sync before state/vamc: the sync is TextIt-wins and would clobber
#     fresh state/vamc with TextIt's older values if it ran after.
#   - state/vamc before vamc-sync: vamc-sync derives display names from the
#     vamc_presumed values state/vamc writes.
#   - state-vamc-sync's TextIt-writeback EXISTS-filter reads itdo423_textit_full,
#     which contacts-sync (step 2) freshly rewrote this same run — so it's current.
#
# Each child is --no-allow-unauthenticated. The orchestrator mints a GCP OIDC
# identity token per call (audience = child base URL) using its runtime SA, and
# ALSO passes each child's app-password in the body (the second auth layer the
# children enforce). The runtime SA needs roles/run.invoker on all four children.
#
# Gating: a step is a PASS only if the HTTP call returns 2xx AND the child's JSON
# `status` is not "error". Any failure HALTS the chain by default (a failed
# backup must not let writes proceed; a failed contacts-sync must not let
# state/vamc run on stale data). Pass {"continue_on_error": true} to run all
# steps regardless and report per-step outcomes (diagnostic use only).
# ---------------------------------------------------------------------------

ORCH_PASSWORD = os.environ.get("ORCH_PASSWORD", "")

# Per-child config: (name, base_url, endpoint, password_env, extra_body)
# base_url is the OIDC audience; full URL = base_url + endpoint.
STEPS = [
    {
        "name": "backup-textit-flows",
        "base_url": os.environ.get("BACKUP_URL", "https://backup-textit-flows-853176470965.us-east1.run.app"),
        "endpoint": "/backup",
        "password_env": "BACKUP_PASSWORD",
    },
    {
        "name": "contacts-sync",
        "base_url": os.environ.get("CONTACTS_URL", "https://contacts-sync-853176470965.us-east1.run.app"),
        "endpoint": "/sync",
        "password_env": "CONTACTS_PASSWORD",
    },
    {
        "name": "state-vamc-sync",
        "base_url": os.environ.get("STATEVAMC_URL", "https://state-vamc-sync-853176470965.us-east1.run.app"),
        "endpoint": "/sync",
        "password_env": "STATEVAMC_PASSWORD",
    },
    {
        "name": "vamc-sync",
        "base_url": os.environ.get("VAMC_URL", "https://vamc-sync-853176470965.us-east1.run.app"),
        "endpoint": "/sync",
        "password_env": "VAMC_PASSWORD",
    },
]

# Per-step HTTP timeout (seconds). The contacts-sync + state/vamc steps are the
# long ones (full contact pull, per-zip lookups, throttled TextIt writes).
STEP_TIMEOUT = int(os.environ.get("STEP_TIMEOUT", "3600"))


def mint_oidc_token(audience):
    """Mint a GCP OIDC identity token for the given audience using the runtime
    service account (ambient ADC). Used to satisfy each child's
    --no-allow-unauthenticated GCP auth layer."""
    return google_id_token.fetch_id_token(GoogleAuthRequest(), audience)


def call_step(step):
    """Call one child service. Returns (passed: bool, detail: dict)."""
    url = step["base_url"] + step["endpoint"]
    password = os.environ.get(step["password_env"], "")
    body = {"password": password}

    started = time.time()
    try:
        token = mint_oidc_token(step["base_url"])
    except Exception as e:
        return False, {"step": step["name"], "outcome": "auth_error",
                       "error": f"OIDC mint failed: {e}", "elapsed_sec": 0}

    try:
        resp = requests.post(
            url,
            json=body,
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            timeout=STEP_TIMEOUT,
        )
    except Exception as e:
        return False, {"step": step["name"], "outcome": "request_error",
                       "error": str(e), "elapsed_sec": round(time.time() - started, 1)}

    elapsed = round(time.time() - started, 1)

    # Parse child JSON if present
    try:
        payload = resp.json()
    except Exception:
        payload = {"raw": resp.text[:500]}

    http_ok = 200 <= resp.status_code < 300
    child_status = payload.get("status") if isinstance(payload, dict) else None
    # PASS = 2xx AND child status not "error". "partial" (e.g. vamc-sync phase1
    # sheet read failed but phase2 ran) is treated as a non-fatal pass but flagged.
    passed = http_ok and child_status != "error"

    detail = {
        "step": step["name"],
        "outcome": "pass" if passed else "fail",
        "http_status": resp.status_code,
        "child_status": child_status,
        "child_response": payload,
        "elapsed_sec": elapsed,
    }
    return passed, detail


def run_pipeline(continue_on_error=False):
    run_id = "orch_" + datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    logger.info(f"pipeline start run_id={run_id}")
    results = []
    halted_at = None

    for step in STEPS:
        logger.info(f"  -> {step['name']}")
        passed, detail = call_step(step)
        results.append(detail)
        logger.info(f"  <- {step['name']}: {detail['outcome']} "
                    f"(http={detail.get('http_status')}, {detail.get('elapsed_sec')}s)")
        if not passed and not continue_on_error:
            halted_at = step["name"]
            logger.error(f"HALT after {step['name']} — chain stopped (continue_on_error=false)")
            break

    overall = "success" if all(r["outcome"] == "pass" for r in results) and halted_at is None else "failed"
    return {
        "status": overall,
        "run_id": run_id,
        "halted_at": halted_at,
        "steps_run": len(results),
        "steps_total": len(STEPS),
        "results": results,
    }


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200


@app.route("/run", methods=["POST"])
def run():
    body = request.get_json(force=True, silent=True) or {}
    if ORCH_PASSWORD and body.get("password") != ORCH_PASSWORD:
        return jsonify({"status": "error", "message": "Unauthorized"}), 403
    continue_on_error = bool(body.get("continue_on_error", False))
    try:
        result = run_pipeline(continue_on_error=continue_on_error)
        code = 200 if result["status"] == "success" else 502
        return jsonify(result), code
    except Exception as e:
        logger.exception("orchestrator failed")
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
