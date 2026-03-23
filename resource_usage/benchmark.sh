#!/bin/bash
# =============================================================================
# run_benchmark.sh
# Automated benchmarking script for MATLAB source vs nf-core pipeline
#
# Runs N replicates of both implementations in randomised order to control
# for time-of-day effects, thermal throttling, and background system load.
#
# Each replicate consists of:
#   MATLAB : 3 sequential process calls (intensity, align, stitch)
#            monitored via matlab_monitor.sh
#   Nextflow: 1 full pipeline run (all 3 processes)
#            monitored via native Nextflow trace
#
# Output structure:
#   BASE_OUTDIR/
#     matlab/
#       replicate_01/
#         process_intensity/   <- matlab_monitor.sh output
#         process_align/
#         process_stitch/
#       replicate_02/
#       ...
#     nextflow/
#       replicate_01/          <- nextflow outdir (trace file inside)
#         work/                <- nextflow workdir
#       replicate_02/
#       ...
#     benchmark_run.log        <- full log of all runs with timestamps
#     run_order.txt            <- the randomised run order used
# =============================================================================

# --- Configuration -----------------------------------------------------------
N_REPLICATES=30
BASE_OUTDIR="./benchmark_results"

# MATLAB
MONITOR_SCRIPT="./matlab_monitor.sh"

# Nextflow
NF_INPUT="/home/schwitalla/Documents/sample_sheettest_local.csv"
NF_PIPELINE="lsmquant"
NF_PROFILE="docker"
NF_STAGE="int-align-stitch"

# --- Logging -----------------------------------------------------------------
LOG_FILE=""   # set after BASE_OUTDIR is created below

log() {
    local msg="[$(date '+%Y-%m-%d %H:%M:%S')] $1"
    echo "$msg"
    echo "$msg" >> "$LOG_FILE"
}

log_separator() {
    local line="$(printf '=%.0s' {1..60})"
    echo "$line"
    echo "$line" >> "$LOG_FILE"
}

# --- Preflight checks --------------------------------------------------------
preflight_checks() {
    local errors=0

    if [ ! -f "$MONITOR_SCRIPT" ]; then
        echo "ERROR: matlab_monitor.sh not found at: $MONITOR_SCRIPT"
        errors=$((errors + 1))
    fi

    if [ ! -x "$MONITOR_SCRIPT" ]; then
        echo "ERROR: matlab_monitor.sh is not executable. Run: chmod +x $MONITOR_SCRIPT"
        errors=$((errors + 1))
    fi

    if ! command -v nextflow &>/dev/null; then
        echo "ERROR: nextflow not found in PATH"
        errors=$((errors + 1))
    fi

    if [ ! -f "$NF_INPUT" ]; then
        echo "ERROR: Nextflow input file not found: $NF_INPUT"
        errors=$((errors + 1))
    fi

    if [ $errors -gt 0 ]; then
        echo "Aborting: $errors preflight check(s) failed."
        exit 1
    fi
}

# --- Random order generator --------------------------------------------------
# Generates a randomised sequence of run tokens:
#   "matlab_01" "nextflow_01" "matlab_02" "nextflow_02" ... shuffled
generate_run_order() {
    local n=$1
    local tokens=()
    for i in $(seq 1 $n); do
        local idx=$(printf '%02d' $i)
        tokens+=("matlab_${idx}")
        tokens+=("nextflow_${idx}")
    done

    # Fisher-Yates shuffle using awk
    printf '%s\n' "${tokens[@]}" | awk 'BEGIN{srand()} {lines[NR]=$0}
        END{
            for(i=NR;i>1;i--){
                j=int(rand()*i)+1;
                tmp=lines[i]; lines[i]=lines[j]; lines[j]=tmp
            }
            for(i=1;i<=NR;i++) print lines[i]
        }'
}

# --- MATLAB replicate runner -------------------------------------------------
run_matlab_replicate() {
    local rep_idx=$1   # zero-padded, e.g. "01"
    local outdir="$BASE_OUTDIR/matlab/replicate_${rep_idx}"

    log "--- MATLAB replicate ${rep_idx} START ---"
    log "Output dir: $outdir"

    # Run all 3 processes sequentially, each in its own monitored subdirectory
    local processes=("intensity" "align" "stitch")
    local commands=(
        "NM_config('intensity','sample${rep_idx}',true)"
        "NM_config('align','sample${rep_idx}',true)"
        "NM_config('stitch','sample${rep_idx}',true)"
    )

    for i in "${!processes[@]}"; do
        local proc="${processes[$i]}"
        local cmd="${commands[$i]}"
        local proc_outdir="$outdir/process_${proc}"

        log "  Running MATLAB process: $proc"
        log "  Command: $cmd"

        bash "$MONITOR_SCRIPT" \
            -c "$cmd" \
            -o "$proc_outdir"

        local exit_code=$?
        if [ $exit_code -ne 0 ]; then
            log "ERROR: MATLAB process '$proc' failed (exit code $exit_code) on replicate ${rep_idx}"
            log "Stopping benchmark run."
            exit 1
        fi

        log "  MATLAB process $proc complete."
    done

    log "--- MATLAB replicate ${rep_idx} DONE ---"
}

# --- Nextflow replicate runner -----------------------------------------------
run_nextflow_replicate() {
    local rep_idx=$1   # zero-padded, e.g. "01"
    local outdir="$BASE_OUTDIR/nextflow/replicate_${rep_idx}"
    local workdir="$outdir/work"

    log "--- Nextflow replicate ${rep_idx} START ---"
    log "Output dir: $outdir"
    log "Work dir:   $workdir"

    mkdir -p "$outdir"

    nextflow run "$NF_PIPELINE" \
        -profile "$NF_PROFILE" \
        --input "$NF_INPUT" \
        --outdir "$outdir" \
        -work-dir "$workdir" \
        --stage "$NF_STAGE"

    local exit_code=$?
    if [ $exit_code -ne 0 ]; then
        log "ERROR: Nextflow replicate ${rep_idx} failed (exit code $exit_code)"
        log "Stopping benchmark run."
        exit 1
    fi

    log "--- Nextflow replicate ${rep_idx} DONE ---"
}

# --- Progress tracker --------------------------------------------------------
# Writes a simple progress file so you can check status mid-run
update_progress() {
    local completed=$1
    local total=$2
    local current_token=$3
    local progress_file="$BASE_OUTDIR/progress.txt"
    {
        echo "Last updated: $(date '+%Y-%m-%d %H:%M:%S')"
        echo "Completed: $completed / $total runs"
        echo "Last completed: $current_token"
        echo "Remaining: $(( total - completed ))"
    } > "$progress_file"
}

# --- Main --------------------------------------------------------------------
main() {
    # Parse optional arguments
    while getopts "n:o:h" opt; do
        case $opt in
            n) N_REPLICATES="$OPTARG" ;;
            o) BASE_OUTDIR="$OPTARG"  ;;
            h)
                echo "Usage: $0 [-n replicates] [-o output_dir]"
                echo "  -n  Number of replicates (default: 30)"
                echo "  -o  Base output directory (default: ./benchmark_results)"
                exit 0
                ;;
        esac
    done

    # Set up output directories and logging
    mkdir -p "$BASE_OUTDIR/matlab"
    mkdir -p "$BASE_OUTDIR/nextflow"
    LOG_FILE="$BASE_OUTDIR/benchmark_run.log"
    touch "$LOG_FILE"

    log_separator
    log "Benchmark run started"
    log "N replicates:   $N_REPLICATES"
    log "Base output:    $BASE_OUTDIR"
    log "Nextflow input: $NF_INPUT"
    log_separator

    # Preflight
    preflight_checks

    # Generate and save randomised run order
    local run_order_file="$BASE_OUTDIR/run_order.txt"
    local run_order=()
    while IFS= read -r token; do
        run_order+=("$token")
    done < <(generate_run_order "$N_REPLICATES")

    printf '%s\n' "${run_order[@]}" > "$run_order_file"
    log "Randomised run order saved to: $run_order_file"
    log "Total runs to execute: ${#run_order[@]}"
    log_separator

    # Execute in randomised order
    local completed=0
    local total=${#run_order[@]}

    for token in "${run_order[@]}"; do
        local tool="${token%_*}"        # "matlab" or "nextflow"
        local rep_idx="${token#*_}"     # "01", "02", etc.

        log_separator
        log "Run $((completed + 1)) / $total : $token"
        log_separator

        if [ "$tool" = "matlab" ]; then
            run_matlab_replicate "$rep_idx"
        elif [ "$tool" = "nextflow" ]; then
            run_nextflow_replicate "$rep_idx"
        fi

        completed=$((completed + 1))
        update_progress "$completed" "$total" "$token"
        log "Completed $completed / $total runs"
    done

    log_separator
    log "Benchmark run complete."
    log "Total runs completed: $completed / $total"
    log "Results in: $BASE_OUTDIR"
    log_separator
}

main "$@"