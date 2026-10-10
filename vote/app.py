from flask import Flask, render_template, request, make_response, jsonify, g
from redis import Redis
import os
import socket
import random
import json
import logging
import threading
import time
import uuid

# Configure Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("vote_app")

option_a = os.getenv("OPTION_A", "Cats")
option_b = os.getenv("OPTION_B", "Dogs")
hostname = socket.gethostname()

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
AUTO_VOTE_INTERVAL = float(os.getenv("AUTO_VOTE_INTERVAL", "5.0"))
AUTO_VOTE_ENABLED = os.getenv("AUTO_VOTE_ENABLED", "true").lower() in ("true", "1", "yes")

app = Flask(__name__)

# State tracking for auto-voting engine
auto_voter_state = {
    "enabled": AUTO_VOTE_ENABLED,
    "interval": AUTO_VOTE_INTERVAL,
    "votes_emitted": 0,
    "last_vote": None,
    "last_vote_time": None,
}


# Global Redis client with connection pooling
redis_client = None

def get_redis_client():
    global redis_client
    if redis_client is None:
        redis_client = Redis(host=REDIS_HOST, db=0, socket_timeout=3, retry_on_timeout=True)
    return redis_client


def push_vote_to_redis(voter_id: str, vote_choice: str, is_simulated: bool = False):
    """Pushes a vote JSON payload into the Redis `votes` queue for the worker."""
    client = get_redis_client()
    payload = json.dumps({
        "voter_id": voter_id,
        "vote": vote_choice,
        "is_simulated": is_simulated,
    })
    client.rpush("votes", payload)
    return payload


def background_auto_voter():
    """
    Background daemon thread that continuously generates random votes every 5 seconds.
    Ensures telemetry flows through the pipeline into Grafana live even without active browser sessions.
    """
    log.info("Starting background auto-voter thread (interval=%.1fs, enabled=%s)...",
             auto_voter_state["interval"], auto_voter_state["enabled"])
    time.sleep(5)  # Initial grace period for Redis & Worker to initialize

    while True:
        try:
            if auto_voter_state["enabled"]:
                sim_voter_id = f"sim-{uuid.uuid4().hex[:10]}"
                # 50/50 random selection between 'a' (Cats) and 'b' (Dogs)
                chosen_vote = random.choice(["a", "b"])
                label = option_a if chosen_vote == "a" else option_b

                push_vote_to_redis(sim_voter_id, chosen_vote, is_simulated=True)

                auto_voter_state["votes_emitted"] += 1
                auto_voter_state["last_vote"] = label
                auto_voter_state["last_vote_time"] = time.strftime("%Y-%m-%d %H:%M:%S")

                log.info("⚡ [Auto-Voter Engine] Produced vote #%d: %s (%s) for voter %s",
                         auto_voter_state["votes_emitted"], label, chosen_vote, sim_voter_id)

            interval = max(1.0, float(auto_voter_state["interval"]))
            time.sleep(interval)
        except Exception as exc:
            log.warning("Auto-voter loop warning (will retry): %s", exc)
            time.sleep(3)


# Launch background daemon thread
voter_thread = threading.Thread(target=background_auto_voter, daemon=True)
voter_thread.start()


@app.route("/", methods=["POST", "GET"])
def index():
    voter_id = request.cookies.get("voter_id")
    if not voter_id:
        voter_id = hex(random.getrandbits(64))[2:-1]

    vote = None

    if request.method == "POST":
        vote = request.form.get("vote")
        if vote in ("a", "b"):
            push_vote_to_redis(voter_id, vote, is_simulated=False)
            log.info("Manual vote received: voter=%s choice=%s (%s)",
                     voter_id, vote, option_a if vote == "a" else option_b)

    resp = make_response(render_template(
        "index.html",
        option_a=option_a,
        option_b=option_b,
        hostname=hostname,
        vote=vote,
        voter_id=voter_id,
        auto_vote_enabled=auto_voter_state["enabled"],
        auto_vote_interval=auto_voter_state["interval"],
    ))
    resp.set_cookie("voter_id", voter_id, max_age=3600*24*30)
    return resp


@app.route("/api/vote", methods=["POST"])
def api_vote():
    """API endpoint for direct/client-side voting."""
    data = request.get_json(silent=True) or request.form
    is_simulated = bool(data.get("is_simulated", False))
    voter_id = data.get("voter_id")
    if not voter_id and not is_simulated:
        voter_id = request.cookies.get("voter_id")
    if not voter_id:
        voter_id = hex(random.getrandbits(64))[2:-1]

    vote = data.get("vote")
    if vote not in ("a", "b"):
        return jsonify({"error": "Invalid vote. Must be 'a' or 'b'"}), 400

    push_vote_to_redis(voter_id, vote, is_simulated=is_simulated)
    label = option_a if vote == "a" else option_b

    resp = make_response(jsonify({
        "status": "success",
        "voter_id": voter_id,
        "vote": vote,
        "label": label,
        "is_simulated": is_simulated,
        "timestamp": time.time(),
    }))
    if not is_simulated:
        resp.set_cookie("voter_id", voter_id, max_age=3600*24*30)
    return resp


@app.route("/api/status", methods=["GET"])
def api_status():
    """Returns runtime telemetry for the voting app UI."""
    return jsonify({
        "hostname": hostname,
        "option_a": option_a,
        "option_b": option_b,
        "auto_voter": auto_voter_state,
        "server_time": time.time(),
    })


@app.route("/api/stats", methods=["GET"])
def api_stats():
    """Returns runtime state for live UI updates."""
    return jsonify({
        "option_a": option_a,
        "option_b": option_b,
        "auto_voter": auto_voter_state,
        "server_time": time.time(),
    })


@app.route("/api/auto-vote/toggle", methods=["POST"])
def api_auto_vote_toggle():
    """Toggles or configures the auto-voter engine."""
    data = request.get_json(silent=True) or {}
    if "enabled" in data:
        auto_voter_state["enabled"] = bool(data["enabled"])
    else:
        auto_voter_state["enabled"] = not auto_voter_state["enabled"]

    if "interval" in data:
        try:
            val = float(data["interval"])
            if val >= 1.0:
                auto_voter_state["interval"] = val
        except ValueError:
            pass

    log.info("Auto-voter updated: enabled=%s, interval=%.1fs",
             auto_voter_state["enabled"], auto_voter_state["interval"])

    return jsonify({
        "status": "success",
        "auto_voter": auto_voter_state,
    })


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=80, debug=True, threaded=True)
