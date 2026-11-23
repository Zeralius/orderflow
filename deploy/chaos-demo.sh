#!/usr/bin/env bash
# Chaos demo: kills parts of the running system in the middle of orders and shows that every
# order still finishes, none twice. Runs against Docker Compose:
#
#   ./mvnw -q package -DskipTests && docker compose --profile apps up -d --build --wait
#   deploy/chaos-demo.sh
#
# Scenes: 1) payment-service killed, 2) Kafka killed, 3) order-service (the saga itself) killed.
# Set COMPOSE_PROJECT if you started Compose with -p.
set -euo pipefail
cd "$(dirname "$0")/.."

ORDERS_PER_SCENE=${ORDERS_PER_SCENE:-5}
API=http://localhost:8080/api

compose() { docker compose ${COMPOSE_PROJECT:+-p "$COMPOSE_PROJECT"} --profile apps "$@"; }

token() {
  curl -sf localhost:8180/realms/orderflow/protocol/openid-connect/token \
    -d grant_type=password -d client_id=orderflow-demo -d username="$1" -d password="$1" \
    | sed -E 's/.*"access_token":"([^"]+)".*/\1/'
}
TOKEN=$(token alice)

# Places an order for one book, prints its URL (the Location header)
place() {
  curl -sf -o /dev/null -D - -X POST "$API/orders" \
    -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
    -d '{"lines":[{"sku":"BOOK-JAVA25","quantity":1}]}' \
    | tr -d '\r' | sed -n 's/^[Ll]ocation: //p'
}

status() {
  local body
  body=$(curl -sf "$1" -H "Authorization: Bearer $TOKEN") || { echo "UNREACHABLE"; return; }
  sed -E 's/.*"status":"([A-Z_]+)".*/\1/' <<< "$body"
}

# "3 CONFIRMED, 2 STOCK_RESERVED"
summary() {
  for url in "$@"; do status "$url"; done | sort | uniq -c | awk '{printf "%s%s %s", sep, $1, $2; sep=", "}'
}

# Waits until every order has reached its final state, prints how long that took
wait_until_final() {
  local start=$SECONDS
  while (( SECONDS - start < 180 )); do
    local open=0
    for url in "$@"; do
      case $(status "$url") in CONFIRMED|CANCELLED) ;; *) open=$((open + 1)) ;; esac
    done
    if (( open == 0 )); then
      echo "    all final after $((SECONDS - start)) s: $(summary "$@")"
      return 0
    fi
    sleep 1
  done
  echo "    NOT all final after 180 s: $(summary "$@")"
  return 1
}

# Messages written by order-service but not yet sent to Kafka
outbox_pending() {
  curl -sf localhost:8081/actuator/prometheus | awk '/^orderflow_outbox_pending/ {print int($2)}' || echo "?"
}

wait_healthy() {
  local port=$1
  until curl -sf "localhost:$port/actuator/health" > /dev/null; do sleep 1; done
}

place_orders() {
  ORDERS=()
  for _ in $(seq 1 "$ORDERS_PER_SCENE"); do ORDERS+=("$(place)"); done
  echo "    $ORDERS_PER_SCENE orders accepted (202)"
}

echo "== Scene 1: payment-service is killed (SIGKILL), orders keep coming"
compose kill payment-service > /dev/null 2>&1
place_orders
sleep 5
echo "    5 s later: $(summary "${ORDERS[@]}")   <- stock reserved, payment waiting in Kafka"
compose start payment-service > /dev/null 2>&1
echo "    payment-service restarted"
wait_until_final "${ORDERS[@]}"

echo
echo "== Scene 2: Kafka is killed, orders keep coming"
compose kill kafka > /dev/null 2>&1
place_orders
sleep 3
echo "    3 s later: $(summary "${ORDERS[@]}"), outbox backlog at order-service: $(outbox_pending)"
compose start kafka > /dev/null 2>&1
echo "    Kafka restarted"
wait_until_final "${ORDERS[@]}"
echo "    outbox backlog now: $(outbox_pending)"

echo
echo "== Scene 3: order-service, which runs the saga, is killed while its orders are in flight"
compose pause inventory-service > /dev/null 2>&1
place_orders
echo "    inventory-service frozen for a moment, so the orders wait: $(summary "${ORDERS[@]}")"
compose kill order-service > /dev/null 2>&1
echo "    order-service killed (SIGKILL)"
compose unpause inventory-service > /dev/null 2>&1
sleep 3
echo "    inventory has answered meanwhile; the replies wait in Kafka for a saga that is down"
compose start order-service > /dev/null 2>&1
wait_healthy 8081
echo "    order-service restarted, picks up the saga state from its database"
wait_until_final "${ORDERS[@]}"

echo
# Each order has at most one payment: count payments against the orders they belong to
read -r payments paid_orders < <(compose exec -T postgres psql -U orderflow -d payments -tA -F ' ' \
  -c "select count(*), count(distinct order_id) from payments")
if (( payments != paid_orders )); then
  echo "!! $payments payments for $paid_orders orders: some order was charged twice"
  exit 1
fi
echo "Every order finished, none was lost. Checked: $payments payments for $paid_orders orders, none charged twice."
