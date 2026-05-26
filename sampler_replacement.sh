#!/bin/bash
# =============================================================
# REPLACEMENT SAMPLER SECTION for run_hetero_plainmsg_raft.sh
#
# Replace the entire block from "TPS SAMPLER CONFIGURATION"
# through the end of inject_event, run_crash_case_sampled,
# run_d1_case_sampled, and run_d4_case_sampled.
#
# KEY CHANGE: sampling is now done in-process by the clients.
# This shell helper only injects event labels into each client VM.
# Each client writes tps_timeline_<timestamp>.csv under ./eval/client<ID>/.
# =============================================================

# ================================================================
# TPS SAMPLER CONFIGURATION
# ================================================================
_SAMPLER_PID=""
_SAMPLER_CSV=""

# ----------------------------------------------------------------
# start_tps_sampler <csv_path> [label]
# ----------------------------------------------------------------
start_tps_sampler() {
    :
}

# ----------------------------------------------------------------
# stop_tps_sampler
# ----------------------------------------------------------------
stop_tps_sampler() {
    :
}

# ----------------------------------------------------------------
# inject_event <label>
# ----------------------------------------------------------------
inject_event() {
    local label="$1"
    local num_servers="${NUM_SERVERS:-${#SERVER_IPS[@]}}"
    for i in "${!CLIENT_IPS[@]}"; do
        local cid=$(( num_servers + i ))
        local event_path="${REMOTE_DIR}/eval/client${cid}/.event"
        ssh -o ConnectTimeout=3 -o BatchMode=yes \
            -i "$SSH_KEY" "$USER@${CLIENT_IPS[$i]}" \
            "mkdir -p '${REMOTE_DIR}/eval/client${cid}' && printf '%s\n' '${label}' > '${event_path}'" \
            >/dev/null 2>&1 &
    done
    wait
}

# ================================================================
# run_crash_case_sampled
# node_spec: no_failure | leader | follower:<id> | f_of_n:<count>
# ================================================================
run_crash_case_sampled() {
    local label=$1
    local node_spec=$2
    local crash_trigger="${CRASH_TRIGGER_SECONDS:-10}"

    echo ""
    echo "=================================================="
    echo "Running (sampled): $label  [crash: ${node_spec} at t=${crash_trigger}s]"
    echo "=================================================="

    local dest_dir="${RUN_DIR}/${label}"
    mkdir -p "$dest_dir"
    CLUSTER_ACTIVE=true
    env "${BASE_ENV[@]}" bash "$START_SCRIPT"
    inject_event "stable"

    echo "  [crash] Waiting ${crash_trigger}s before fault injection..."
    sleep "$crash_trigger"

    local kind="${node_spec%%:*}"
    local arg="${node_spec#*:}"

    case "$kind" in
        no_failure)
            inject_event "no_failure_baseline"
            echo "  [crash] No failure — baseline run"
            ;;
        leader)
            inject_event "crash_leader"
            kill_cabinet_on_node "${SERVER_IPS[0]}" "leader"
            echo "  [crash] Leader killed at $(date '+%H:%M:%S')"
            ;;
        follower)
            inject_event "crash_follower${arg}"
            kill_cabinet_on_node "${SERVER_IPS[$arg]}" "server${arg}"
            echo "  [crash] Follower ${arg} killed at $(date '+%H:%M:%S')"
            ;;
        f_of_n)
            local available=()
            for i in "${!SERVER_IPS[@]}"; do [ "$i" -eq 0 ] && continue; available+=("$i"); done
            local killed=()
            for (( k=0; k<arg; k++ )); do
                local pick=$(( RANDOM % ${#available[@]} ))
                killed+=("${available[$pick]}")
                available=("${available[@]:0:$pick}" "${available[@]:$(( pick+1 ))}")
            done
            inject_event "crash_f${arg}"
            for fid in "${killed[@]}"; do kill_cabinet_on_node "${SERVER_IPS[$fid]}" "server${fid}" & done
            wait
            echo "  [crash] Killed: ${killed[*]} at $(date '+%H:%M:%S')"
            ;;
        *) echo "  ERROR: unknown crash spec '$node_spec'"; return 1 ;;
    esac

    inject_event "post_crash"
    sleep "$RUNTIME_SECONDS"
    stop_plain_cluster
    CLUSTER_ACTIVE=false
    archive_latest_result "$label"
}

# ================================================================
# run_d1_case_sampled — steady-state TPS at a fixed uniform delay
# ================================================================
run_d1_case_sampled() {
    local label=$1
    local delay_ms=$2
    local jitter_ms=$3

    echo ""
    echo "=================================================="
    echo "Running (sampled): $label  [D1 ${delay_ms}ms ±${jitter_ms}ms]"
    echo "=================================================="

    local dest_dir="${RUN_DIR}/${label}"
    mkdir -p "$dest_dir"
    apply_uniform_delay "$delay_ms" "$jitter_ms"

    CLUSTER_ACTIVE=true
    env "${BASE_ENV[@]}" bash "$START_SCRIPT"
    inject_event "delay_${delay_ms}ms"

    sleep "$RUNTIME_SECONDS"

    remove_all_delay
    stop_plain_cluster
    CLUSTER_ACTIVE=false
    archive_latest_result "$label"
}

# ================================================================
# run_d4_case_sampled — calm/burst cycling with event annotations
# ================================================================
run_d4_case_sampled() {
    local label=$1
    local calm_duration="${2:-10}"
    local burst_duration="${3:-5}"
    local runtime_override="${4:-$RUNTIME_SECONDS}"

    echo ""
    echo "=================================================="
    echo "Running (sampled): $label  [D4 ${calm_duration}s calm / ${burst_duration}s burst]"
    echo "=================================================="

    local dest_dir="${RUN_DIR}/${label}"
    mkdir -p "$dest_dir"
    cache_all_interfaces
    remove_all_delay

    CLUSTER_ACTIVE=true
    env "${BASE_ENV[@]}" bash "$START_SCRIPT"
    inject_event "calm_start"

    local elapsed=0 cycle=0

    while [ "$elapsed" -lt "$runtime_override" ]; do
        # CALM
        inject_event "calm_c${cycle}"
        for i in "${!SERVER_IPS[@]}"; do
            ssh -i "$SSH_KEY" "$USER@${SERVER_IPS[$i]}" \
                "sudo tc qdisc del dev '${_CACHED_SERVER_IFACES[$i]}' root 2>/dev/null || true" || true &
        done
        for i in "${!CLIENT_IPS[@]}"; do
            ssh -i "$SSH_KEY" "$USER@${CLIENT_IPS[$i]}" \
                "sudo tc qdisc del dev '${_CACHED_CLIENT_IFACES[$i]}' root 2>/dev/null || true" || true &
        done
        wait
        local calm_sleep=$(( calm_duration < (runtime_override - elapsed) ? calm_duration : (runtime_override - elapsed) ))
        sleep "$calm_sleep"; elapsed=$(( elapsed + calm_sleep ))
        [ "$elapsed" -ge "$runtime_override" ] && break

        # BURST
        inject_event "burst_c${cycle}"
        for i in "${!SERVER_IPS[@]}"; do
            ssh -i "$SSH_KEY" "$USER@${SERVER_IPS[$i]}" \
                "sudo tc qdisc del dev '${_CACHED_SERVER_IFACES[$i]}' root 2>/dev/null || true
                 sudo tc qdisc add dev '${_CACHED_SERVER_IFACES[$i]}' root netem delay 1000ms 100ms distribution normal" \
                || true &
        done
        for i in "${!CLIENT_IPS[@]}"; do
            ssh -i "$SSH_KEY" "$USER@${CLIENT_IPS[$i]}" \
                "sudo tc qdisc del dev '${_CACHED_CLIENT_IFACES[$i]}' root 2>/dev/null || true
                 sudo tc qdisc add dev '${_CACHED_CLIENT_IFACES[$i]}' root netem delay 1000ms 100ms distribution normal" \
                || true &
        done
        wait
        local burst_sleep=$(( burst_duration < (runtime_override - elapsed) ? burst_duration : (runtime_override - elapsed) ))
        sleep "$burst_sleep"; elapsed=$(( elapsed + burst_sleep ))
        cycle=$(( cycle + 1 ))
    done
    remove_all_delay
    stop_plain_cluster
    CLUSTER_ACTIVE=false
    archive_latest_result "$label"
}
