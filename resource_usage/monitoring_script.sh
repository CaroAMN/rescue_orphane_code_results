#!/bin/bash
# =============================================================================
# matlab_monitor.sh
# Resource monitoring script for MATLAB pipelines
#
# Derived from Nextflow's .command.run monitoring logic (nxf_mem_watch,
# nxf_trace_linux) to produce output comparable to a Nextflow trace file.
#
# Key design decisions:
#   - Memory polling uses the same adaptive intervals as Nextflow (1s/5s/30s)
#   - Memory watcher uses read -t + a pipe signal to terminate immediately
#     when the MATLAB process ends (mirrors nxf_mem_watch signal mechanism)
#   - CPU usage is computed as a delta of /proc/stat and /proc/<pid>/stat
#     across the full runtime (mirrors nxf_trace_linux cpu calculation)
#   - I/O stats are captured as deltas from /proc/<pid>/io
#     (mirrors nxf_trace_linux io_stat calculation)
#   - Output trace file format matches Nextflow's nextflow.trace/v2 format
#   - Additionally writes a time-series CSV for memory-over-time analysis
#
# Output files (in OUTPUT_DIR):
#   matlab_trace.log          - Nextflow-compatible summary trace (one row)
#   matlab_resource_usage.csv - Time-series memory polling data
#   runtime_summary.txt       - Human-readable summary
# =============================================================================

# --- Default configuration ---------------------------------------------------
OUTPUT_DIR="/mnt/ssd/performance_benchmark/numorph_toolkit/intensity"
MATLAB_PATH="/home/schwitalla/Documents/R2023a/bin/./matlab" 
MATLAB_SCRIPT="/home/schwitalla/Documents/Original_Numorph/numorph_dev/NM_config.m"
NXF_DEBUG=${NXF_DEBUG:=0}
[[ $NXF_DEBUG > 1 ]] && set -x

# --- Timestamp utility (millisecond precision) --------------------------------
# Nextflow's timestamp function
nxf_date() {
    local ts=$(date +%s%3N)
    if [[ ${#ts} == 10 ]];   then echo ${ts}000
    elif [[ $ts == *%3N ]];  then echo ${ts/\%3N/000}
    elif [[ $ts == *3N ]];   then echo ${ts/3N/000}
    elif [[ ${#ts} == 13 ]]; then echo $ts
    else echo "Unexpected timestamp value: $ts"; exit 1
    fi
}

# --- Sleep wrapper ------------------------------------------------------------
nxf_sleep() {
    sleep $1 2>/dev/null || sleep 1
}

# --- Process tree builder -----------------------------------------------------
# Recursively maps parent-child PID relationships and reads /proc/<pid>/status
# for each process in the tree. Identical to Nextflow's nxf_tree.
nxf_tree() {
    local pid=$1

    declare -a ALL_CHILDREN
    while read P PP; do
        ALL_CHILDREN[$PP]+=" $P"
    done < <(ps -e -o pid= -o ppid=)

    pstat() {
        local x_pid=$1
        local STATUS=$(2>/dev/null < /proc/$x_pid/status grep -E 'Vm|ctxt')

        if [ $? = 0 ]; then
            local x_vsz=$(echo  "$STATUS" | grep VmSize | awk '{print $2}'           || echo -n '0')
            local x_rss=$(echo  "$STATUS" | grep VmRSS  | awk '{print $2}'           || echo -n '0')
            local x_peak=$(echo "$STATUS" | grep -E 'VmPeak|VmHWM' \
                             | sed 's/^.*:\s*//' | sed 's/[\sa-zA-Z]*$//' \
                             | tr '\n' ' '                                            || echo -n '0 0')
            local x_pmem=$(awk -v rss=$x_rss -v mem_tot=$mem_tot \
                             'BEGIN {printf "%.0f", rss/mem_tot*100*10}'              || echo -n '0')
            local vol_ctxt=$(echo "$STATUS" | grep '\bvoluntary_ctxt_switches'    | awk '{print $2}' || echo -n '0')
            local inv_ctxt=$(echo "$STATUS" | grep '\bnonvoluntary_ctxt_switches' | awk '{print $2}' || echo -n '0')
            cpu_stat[$x_pid]="$x_pid $x_pmem $x_vsz $x_rss $x_peak $vol_ctxt $inv_ctxt"
        fi
    }

    pwalk() {
        pstat $1
        for i in ${ALL_CHILDREN[$1]:=}; do pwalk $i; done
    }

    pwalk $1
}

# --- Statistics aggregator ----------------------------------------------------
# Sums metrics across all PIDs in the process tree and tracks running peak.
# Identical to Nextflow's nxf_stat.
# Result is stored in global array nxf_stat_ret[1..7]:
#   [1] %mem*10   [2] vmem_kb  [3] rss_kb
#   [4] peak_vmem [5] peak_rss [6] vol_ctxt [7] inv_ctxt
nxf_stat() {
    cpu_stat=()
    nxf_tree $1

    declare -a sum=(0 0 0 0 0 0 0 0)
    local pid
    local i
    for pid in "${!cpu_stat[@]}"; do
        local row=(${cpu_stat[$pid]})
        [ $NXF_DEBUG = 1 ] && echo "++ stat mem=${row[*]}"
        for i in "${!row[@]}"; do
            if [ $i != 0 ]; then
                sum[$i]=$((sum[$i] + row[$i]))
            fi
        done
    done

    [ $NXF_DEBUG = 1 ] && echo -e "++ stat SUM=${sum[*]}"

    for i in {1..7}; do
        if [ ${sum[$i]} -lt ${cpu_peak[$i]} ]; then
            sum[$i]=${cpu_peak[$i]}
        else
            cpu_peak[$i]=${sum[$i]}
        fi
    done

    [ $NXF_DEBUG = 1 ] && echo -e "++ stat PEAK=${sum[*]}\n"
    nxf_stat_ret=(${sum[*]})
}

# --- Memory watcher -----------------------------------------------------------
# Mirrors Nextflow's nxf_mem_watch function exactly:
#   - Same adaptive polling intervals (count-based: 1s / 5s / 30s)
#   - Uses "read -t $timeout -r DONE" so it can be signalled to stop
#     immediately via a pipe (echo 'DONE' >&$mem_fd) rather than waiting
#     out a full sleep interval after the process ends.
#   - Writes memory fields to the trace file on exit.
#   - Also appends each sample to the CSV for time-series analysis.
#
# Arguments: <pid> <start_millis> <csv_file> <trace_file>
nxf_mem_watch() {
    local pid=$1
    local start_millis=$2
    local csv_file=$3
    local trace_file=$4

    local count=0
    declare -a cpu_stat=(0 0 0 0 0 0 0 0)
    declare -a cpu_peak=(0 0 0 0 0 0 0 0)
    local mem_tot=$(< /proc/meminfo grep MemTotal | awk '{print $2}')
    local timeout
    local DONE
    local STOP=''

    while true; do
        nxf_stat $pid

        # Log time-series sample to CSV ( not needed now)
        #local now=$(nxf_date)
        #local elapsed_sec=$(( (now - start_millis) / 1000 ))
        #echo "$now,$elapsed_sec,${nxf_stat_ret[1]},${nxf_stat_ret[2]},${nxf_stat_ret[3]},${nxf_stat_ret[4]},${nxf_stat_ret[5]},${nxf_stat_ret[6]},${nxf_stat_ret[7]}" \
        #    >> "$csv_file"

        # Adaptive interval — identical thresholds to Nextflow
        if   [ $count -lt 10  ]; then timeout=1
        elif [ $count -lt 120 ]; then timeout=5
        else                          timeout=30
        fi

        # read -t: waits up to $timeout seconds for a "DONE" signal on stdin.
        # If the signal arrives (sent by the main loop via the pipe), break
        # immediately. If the timeout expires, continue polling.
        # This mirrors Nextflow's mechanism: the watcher does not have to
        # wait a full sleep interval after the process ends.
        read -t $timeout -r DONE || true
        [[ $DONE ]] && break

        # Fallback: if /proc/<pid> has disappeared and stays gone for >10s,
        # exit anyway. Mirrors Nextflow's STOP sentinel logic.
        if [ ! -e /proc/$pid ]; then
            [ -z "$STOP" ] && STOP=$(nxf_date)
            [ $(( $(nxf_date) - STOP )) -gt 10000 ] && break
        fi

        count=$((count + 1))
    done

    # Write memory fields to the trace file (appended — timing fields written
    # by the main function afterwards, matching Nextflow's two-phase write)
    echo "%mem=${nxf_stat_ret[1]}"       >> "$trace_file"
    echo "vmem=${nxf_stat_ret[2]}"       >> "$trace_file"
    echo "rss=${nxf_stat_ret[3]}"        >> "$trace_file"
    echo "peak_vmem=${nxf_stat_ret[4]}"  >> "$trace_file"
    echo "peak_rss=${nxf_stat_ret[5]}"   >> "$trace_file"
    echo "vol_ctxt=${nxf_stat_ret[6]}"   >> "$trace_file"
    echo "inv_ctxt=${nxf_stat_ret[7]}"   >> "$trace_file"
}

# --- File descriptor finder ---------------------------------------------------
# Finds the next available file descriptor — matches Nextflow's nxf_fd.
nxf_fd() {
    local FD=11
    while [ -e /proc/$$/fd/$FD ]; do FD=$((FD + 1)); done
    echo $FD
}

# --- Main monitoring function -------------------------------------------------
# Mirrors the structure of Nextflow's nxf_trace_linux:
#   1. Snapshot CPU and I/O counters BEFORE launching MATLAB
#   2. Launch MATLAB in the background
#   3. Start nxf_mem_watch in a coprocess connected via a pipe (mem_fd)
#   4. Wait for MATLAB to finish
#   5. Snapshot CPU and I/O counters AFTER MATLAB finishes
#   6. Signal nxf_mem_watch to stop via the pipe
#   7. Compute %cpu as delta(cpu_time) / delta(total_cpu_time)
#   8. Compute I/O deltas
#   9. Write the trace file
monitor_matlab() {
    local matlab_script=$1
    local output_dir=$2

    mkdir -p "$output_dir"

    local trace_file="$output_dir/matlab_trace.log"
    local csv_file="$output_dir/matlab_resource_usage.csv"
    local runtime_file="$output_dir/runtime_summary.txt"

    # Write CSV header
    echo "timestamp_ms,elapsed_sec,mem_pct_x10,vmem_kb,rss_kb,peak_vmem_kb,peak_rss_kb,vol_ctxt,inv_ctxt" \
        > "$csv_file"

    # Initialise trace file (will be appended to by nxf_mem_watch and this fn)
    echo "nextflow.trace/v2" > "$trace_file"

    # --- Pre-launch snapshots (mirrors nxf_trace_linux) ----------------------
    local pid=$$
    local num_cpus=$(< /proc/cpuinfo grep '^processor' -c)
    local cpu_model=$(< /proc/cpuinfo grep '^model name' | head -n 1 \
                        | awk 'BEGIN{FS="\t: "} {print $2}')

    # Total CPU jiffies across all cores (used to normalise %cpu)
    local tot_time0=$(grep '^cpu ' /proc/stat \
                        | awk '{sum=$2+$3+$4+$5+$6+$7+$8+$9; printf "%.0f",sum}')

    # CPU time charged to this shell and its children (in jiffies * 10)
    local cpu_time0=$(2>/dev/null < /proc/$pid/stat \
                        awk '{printf "%.0f", ($16+$17)*10}' || echo -n 'X')

    # I/O counters for this process before launch
    local io_stat0=($(2>/dev/null < /proc/$pid/io \
                        sed 's/^.*:\s*//' | head -n 6 | tr '\n' ' ' \
                        || echo -n '0 0 0 0 0 0'))

    local start_millis=$(nxf_date)

    # --- Launch MATLAB in the background -------------------------------------
    echo "Starting MATLAB: $MATLAB_PATH -nodesktop -nodisplay -nosplash -batch \"NM_config('intensity','test_monitor',true)\""
    echo "Output directory: $output_dir"

    local matlab_expr
    if [ -n "$MATLAB_COMMAND" ]; then
        matlab_expr="$MATLAB_COMMAND"
    else
        matlab_expr="run('$matlab_script')"
    fi
    
    $MATLAB_PATH -nodesktop -nodisplay -nosplash -batch "$matlab_expr" &
    local task=$!
    echo "MATLAB PID: $task"

    # --- Start memory watcher via pipe (mirrors nxf_mem_watch setup) ---------
    # A named pipe (file descriptor mem_fd) connects this shell to
    # nxf_mem_watch. Writing "DONE" to it unblocks the watcher's read -t,
    # causing it to exit immediately rather than waiting a full poll interval.
    mem_fd=$(nxf_fd)
    eval "exec $mem_fd> >(nxf_mem_watch $task $start_millis '$csv_file' '$trace_file')"
    local mem_proc=$!

    # --- Wait for MATLAB to finish -------------------------------------------
    wait $task
    local matlab_exit=$?

    local end_millis=$(nxf_date)

    # --- Post-run CPU snapshot -----------------------------------------------
    local tot_time1=$(grep '^cpu ' /proc/stat \
                        | awk '{sum=$2+$3+$4+$5+$6+$7+$8+$9; printf "%.0f",sum}')
    local cpu_time1=$(2>/dev/null < /proc/$pid/stat \
                        awk '{printf "%.0f", ($16+$17)*10}' || echo -n 'X')

    # %cpu: fraction of total CPU time used, scaled to number of cores,
    # expressed as a percentage. Matches Nextflow's ucpu calculation exactly.
    local ucpu
    if [[ $cpu_time0 == 'X' || $cpu_time1 == 'X' ]]; then
        ucpu=0
    else
        ucpu=$(awk -v p1=$cpu_time1 -v p0=$cpu_time0 \
                   -v t1=$tot_time1 -v t0=$tot_time0 \
                   -v n=$num_cpus \
                   'BEGIN { pct=(p1-p0)/(t1-t0)*100*n; printf("%.0f", pct>0 ? pct : 0) }')
    fi

    # --- Post-run I/O snapshot -----------------------------------------------
    local io_stat1=($(2>/dev/null < /proc/$pid/io \
                        sed 's/^.*:\s*//' | head -n 6 | tr '\n' ' ' \
                        || echo -n '0 0 0 0 0 0'))
    local i
    for i in {0..5}; do
        io_stat1[$i]=$(( io_stat1[$i] - io_stat0[$i] ))
    done

    local wall_time=$(( end_millis - start_millis ))

    # --- Signal memory watcher to stop now -----------------------------------
    # Mirrors: [ -e /proc/$mem_proc ] && eval "echo 'DONE' >&$mem_fd" || true
    [ -e /proc/$mem_proc ] && eval "echo 'DONE' >&$mem_fd" || true
    wait $mem_proc 2>/dev/null || true
    while [ -e /proc/$mem_proc ]; do nxf_sleep 0.1; done

    # --- Write trace file (timing + cpu + io fields) -------------------------
    # Memory fields were already appended by nxf_mem_watch.
    # These fields match Nextflow's nxf_write_trace output exactly.
    echo "realtime=$wall_time"          >> "$trace_file"
    echo "%cpu=$ucpu"                   >> "$trace_file"
    echo "cpu_model=$cpu_model"         >> "$trace_file"
    echo "rchar=${io_stat1[0]}"         >> "$trace_file"
    echo "wchar=${io_stat1[1]}"         >> "$trace_file"
    echo "syscr=${io_stat1[2]}"         >> "$trace_file"
    echo "syscw=${io_stat1[3]}"         >> "$trace_file"
    echo "read_bytes=${io_stat1[4]}"    >> "$trace_file"
    echo "write_bytes=${io_stat1[5]}"   >> "$trace_file"

    # --- Human-readable summary ----------------------------------------------
    local peak_rss_mb=$(( ${cpu_peak[5]:-0} / 1024 ))
    local peak_vmem_mb=$(( ${cpu_peak[4]:-0} / 1024 ))
    local wall_sec=$(( wall_time / 1000 ))

    {
        echo "===== MATLAB Execution Summary ====="
        echo "Script:               $matlab_script"
        echo "Exit status:          $matlab_exit"
        echo "Wall time (ms):       $wall_time"
        echo "Wall time (hh:mm:ss): $(printf '%02d:%02d:%02d' $((wall_sec/3600)) $(( (wall_sec%3600)/60 )) $((wall_sec%60)) )"
        echo "CPU usage:            ${ucpu}%"
        echo "CPU cores available:  $num_cpus"
        echo "CPU model:            $cpu_model"
        echo "Peak RSS:             ${peak_rss_mb} MB"
        echo "Peak virtual mem:     ${peak_vmem_mb} MB"
        echo "Bytes read:           ${io_stat1[4]}"
        echo "Bytes written:        ${io_stat1[5]}"
        echo ""
        echo "Trace file:  $trace_file"
        echo "CSV file:    $csv_file"
    } > "$runtime_file"

    cat "$runtime_file"

    return $matlab_exit
}

# --- Usage -------------------------------------------------------------------
usage() {
    echo "Usage: $0 -s <matlab_script.m> [-o <output_dir>] [-d]"
    echo ""
    echo "Options:"
    echo "  -s SCRIPT   Path to MATLAB script to execute (required)"
    echo "  -o DIR      Output directory (default: ./matlab_monitoring)"
    echo "  -d          Enable debug output (sets NXF_DEBUG=1)"
    echo "  -h          Show this help message"
    exit 1
}

# --- Argument parsing --------------------------------------------------------
while getopts "s:c:o:dh" opt; do
    case $opt in
        s) MATLAB_SCRIPT="$OPTARG" ;;
        c) MATLAB_COMMAND="$OPTARG"   ;;
        o) OUTPUT_DIR="$OPTARG"    ;;
        d) NXF_DEBUG=1             ;;
        h) usage                   ;;
        *) usage                   ;;
    esac
done

# --- Validation --------------------------------------------------------------
if [ -z "$MATLAB_SCRIPT" ]; then
    echo "Error: MATLAB script must be specified with -s"
    usage
fi

if [ ! -f "$MATLAB_SCRIPT" ]; then
    echo "Error: MATLAB script not found: $MATLAB_SCRIPT"
    exit 1
fi

if [ ! -x "$MATLAB_PATH" ]; then
    echo "Error: MATLAB not found or not executable at: $MATLAB_PATH"
    exit 1
fi

# --- Entry point -------------------------------------------------------------
monitor_matlab "$MATLAB_SCRIPT" "$OUTPUT_DIR"