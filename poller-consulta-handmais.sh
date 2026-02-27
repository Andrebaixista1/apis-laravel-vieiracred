#!/usr/bin/env bash
set -u

URL="${HANDMAIS_RUN_URL:-http://127.0.0.1:3002/api/consulta-handmais/run}"
INTERVAL="${HANDMAIS_RUN_INTERVAL_SECONDS:-10}"
MAX_TIME="${HANDMAIS_RUN_MAX_TIME_SECONDS:-120}"
LOCK_FILE="${HANDMAIS_RUN_LOCK_FILE:-/tmp/api-consulta-handmais.lock}"
RESP_FILE="${HANDMAIS_RUN_RESP_FILE:-/tmp/api-consulta-handmais-last.json}"

if command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN="python3"
elif command -v python >/dev/null 2>&1; then
  PYTHON_BIN="python"
else
  PYTHON_BIN=""
fi

exec 9>"$LOCK_FILE"

echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) poller-start url=$URL interval=${INTERVAL}s max_time=${MAX_TIME}s"

while true; do
  ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

  if flock -n 9; then
    code="$(curl -sS --max-time "$MAX_TIME" -o "$RESP_FILE" -w "%{http_code}" "$URL")"
    rc=$?
    if [ "$rc" -ne 0 ]; then
      code="000"
      echo "$ts status=$code error=curl_failed"
      flock -u 9
      sleep "$INTERVAL"
      continue
    fi

    if [ -n "$PYTHON_BIN" ]; then
      summary="$($PYTHON_BIN - "$RESP_FILE" "$code" <<'PY'
import json, sys
path = sys.argv[1]
http_code = sys.argv[2]
try:
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
except Exception as e:
    print(f"status={http_code} parse=json_error error={type(e).__name__}")
    raise SystemExit(0)

ok = data.get('ok')
message = str(data.get('message', '') or '').replace(chr(10), ' ').strip()
fields = {
    'total_logins': data.get('total_logins'),
    'logins_com_saldo': data.get('logins_com_saldo'),
    'pendentes_encontrados': data.get('pendentes_encontrados'),
    'pendentes_alocados': data.get('pendentes_alocados'),
    'processados': data.get('processados'),
    'erros': data.get('erros'),
    'duplicados_criados': data.get('duplicados_criados'),
    'duration_ms': data.get('duration_ms'),
}
parts = [f"status={http_code}", f"ok={ok}"]
for k, v in fields.items():
    if v is not None:
        parts.append(f"{k}={v}")
if message:
    parts.append(f"message={message[:180]}")
print(' '.join(parts))
PY
)"
      echo "$ts $summary"
    else
      echo "$ts status=$code parse=disabled_no_python"
    fi

    flock -u 9
  else
    echo "$ts skip=locked"
  fi

  sleep "$INTERVAL"
done
