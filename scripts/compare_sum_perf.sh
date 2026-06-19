#!/usr/bin/env bash
set -euo pipefail

runs=10
rows=1000000
files=1
sparse_stride=1024
profile=release
print_raw=0

usage() {
  cat <<'USAGE'
Usage: scripts/compare_sum_perf.sh [options]

Runs examples/sum_perf in vortex and regular modes, verifies each result,
and reports timing statistics in milliseconds.

Options:
  --runs N             Runs per mode. Default: 10
  --rows N             Total rows written per run. Default: 1000000
  --files N            Vortex files written per run. Default: 1
  --sparse-stride N    Non-zero patch stride. Default: 1024
  --debug              Use debug profile instead of release
  --print-raw          Print each example run's full stdout
  -h, --help           Show this help
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --runs)
      runs="$2"
      shift 2
      ;;
    --rows)
      rows="$2"
      shift 2
      ;;
    --files)
      files="$2"
      shift 2
      ;;
    --sparse-stride)
      sparse_stride="$2"
      shift 2
      ;;
    --debug)
      profile=debug
      shift
      ;;
    --print-raw)
      print_raw=1
      shift
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    *)
      echo "unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

case "$runs:$rows:$files:$sparse_stride" in
  *[!0-9:]*)
    echo "--runs, --rows, --files, and --sparse-stride must be positive integers" >&2
    exit 2
    ;;
esac

if [[ "$runs" -lt 1 || "$rows" -lt 1 || "$files" -lt 1 || "$sparse_stride" -lt 1 ]]; then
  echo "--runs, --rows, --files, and --sparse-stride must be greater than zero" >&2
  exit 2
fi

build_cmd=(cargo build --example sum_perf)
if [[ "$profile" == "release" ]]; then
  build_cmd+=(--release)
fi

echo "building examples/sum_perf ($profile)..." >&2
"${build_cmd[@]}"

bin="target/${profile}/examples/sum_perf"
if [[ ! -x "$bin" ]]; then
  echo "expected built example at $bin" >&2
  exit 1
fi

tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT
data="$tmpdir/sum_perf.tsv"

printf 'mode\titeration\twrite_ms\tlogical_ms\tphysical_ms\texecution_ms\ttotal_ms\tresult\texpected\n' >"$data"

printf '\nBenchmark config\n'
printf '  profile:        %s\n' "$profile"
printf '  runs per mode:  %s\n' "$runs"
printf '  rows:           %s\n' "$rows"
printf '  files:          %s\n' "$files"
printf '  sparse stride:  %s\n' "$sparse_stride"
printf '\nPer-run timings, milliseconds; each result is checked against expected\n'
printf '%-8s %4s %10s %10s %10s %10s %10s %18s\n' \
  "mode" "run" "write" "logical" "physical" "execution" "total" "result"

parse_run() {
  local mode="$1"
  local iteration="$2"

  awk -v mode="$mode" -v iteration="$iteration" '
    function to_ms(value) {
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", value)
      if (value ~ /ns$/) {
        sub(/ns$/, "", value)
        return value / 1000000
      }
      if (value ~ /µs$/ || value ~ /us$/) {
        sub(/µs$|us$/, "", value)
        return value / 1000
      }
      if (value ~ /ms$/) {
        sub(/ms$/, "", value)
        return value + 0
      }
      if (value ~ /s$/) {
        sub(/s$/, "", value)
        return value * 1000
      }
      return value + 0
    }
    /^write time \(not query\):/ {
      sub(/^write time \(not query\):[[:space:]]*/, "")
      write = to_ms($0)
      next
    }
    /^logical planning:/ {
      sub(/^logical planning:[[:space:]]*/, "")
      logical = to_ms($0)
      next
    }
    /^physical planning:/ {
      sub(/^physical planning:[[:space:]]*/, "")
      physical = to_ms($0)
      next
    }
    /^query execution:/ {
      sub(/^query execution:[[:space:]]*/, "")
      execution = to_ms($0)
      next
    }
    /^query total:/ {
      sub(/^query total:[[:space:]]*/, "")
      total = to_ms($0)
      next
    }
    /^result:/ {
      sub(/^result:[[:space:]]*/, "")
      result = $0
      next
    }
    /^expected:/ {
      sub(/^expected:[[:space:]]*/, "")
      expected = $0
      next
    }
    END {
      if (write == "" || logical == "" || physical == "" || execution == "" || total == "" || result == "" || expected == "") {
        print "failed to parse sum_perf output for " mode " run " iteration > "/dev/stderr"
        exit 3
      }
      if (result != expected) {
        print "result mismatch for " mode " run " iteration ": result=" result " expected=" expected > "/dev/stderr"
        exit 4
      }
      printf "%s\t%d\t%.6f\t%.6f\t%.6f\t%.6f\t%.6f\t%s\t%s\n", mode, iteration, write, logical, physical, execution, total, result, expected
    }
  '
}

for mode in vortex regular; do
  for ((iteration = 1; iteration <= runs; iteration++)); do
    echo "# running mode=${mode} iteration=${iteration}/${runs}" >&2
    output="$("$bin" --mode "$mode" --rows "$rows" --files "$files" --sparse-stride "$sparse_stride")"
    if [[ "$print_raw" -eq 1 ]]; then
      printf '%s\n' "$output"
    fi
    parsed="$(printf '%s\n' "$output" | parse_run "$mode" "$iteration")"
    printf '%s\n' "$parsed" >>"$data"
    IFS=$'\t' read -r parsed_mode parsed_iteration write_ms logical_ms physical_ms execution_ms total_ms result expected <<<"$parsed"
    printf '%-8s %4s %10.3f %10.3f %10.3f %10.3f %10.3f %18s\n' \
      "$parsed_mode" "$parsed_iteration" "$write_ms" "$logical_ms" "$physical_ms" \
      "$execution_ms" "$total_ms" "$result"
  done
done

echo
awk -F '\t' '
  BEGIN {
    metrics[1] = "write"
    metrics[2] = "logical"
    metrics[3] = "physical"
    metrics[4] = "execution"
    metrics[5] = "total"
    fields["write"] = 3
    fields["logical"] = 4
    fields["physical"] = 5
    fields["execution"] = 6
    fields["total"] = 7
  }
  NR == 1 { next }
  {
    mode = $1
    for (i = 1; i <= 5; i++) {
      metric = metrics[i]
      key = mode SUBSEP metric
      value = $(fields[metric]) + 0
      n[key]++
      values[key, n[key]] = value
      sum[key] += value
      sumsq[key] += value * value
      if (!(key in min) || value < min[key]) min[key] = value
      if (!(key in max) || value > max[key]) max[key] = value
    }
  }
  function mean(mode, metric, key) {
    key = mode SUBSEP metric
    return sum[key] / n[key]
  }
  function stdev(mode, metric, key, count, variance) {
    key = mode SUBSEP metric
    count = n[key]
    if (count < 2) return 0
    variance = (sumsq[key] - (sum[key] * sum[key] / count)) / (count - 1)
    if (variance < 0) variance = 0
    return sqrt(variance)
  }
  function median(mode, metric, key, count, i, j, tmp) {
    key = mode SUBSEP metric
    count = n[key]
    delete sorted
    for (i = 1; i <= count; i++) sorted[i] = values[key, i]
    for (i = 1; i <= count; i++) {
      for (j = i + 1; j <= count; j++) {
        if (sorted[j] < sorted[i]) {
          tmp = sorted[i]
          sorted[i] = sorted[j]
          sorted[j] = tmp
        }
      }
    }
    if (count % 2) return sorted[(count + 1) / 2]
    return (sorted[count / 2] + sorted[count / 2 + 1]) / 2
  }
  function print_mode(mode, i, metric, key) {
    printf "%s\n", mode
    printf "metric        mean   median      min      max    stdev\n"
    for (i = 1; i <= 5; i++) {
      metric = metrics[i]
      key = mode SUBSEP metric
      printf "%-10s %8.3f %8.3f %8.3f %8.3f %8.3f\n", metric, mean(mode, metric), median(mode, metric), min[key], max[key], stdev(mode, metric)
    }
    printf "\n"
  }
  END {
    print "Per-mode distribution, milliseconds"
    print_mode("vortex")
    print_mode("regular")

    print "Head-to-head averages, milliseconds"
    printf "%-10s %12s %12s %10s %13s %12s\n", "metric", "vortex avg", "regular avg", "faster", "advantage ms", "advantage %"
    for (i = 1; i <= 5; i++) {
      metric = metrics[i]
      vortex_mean = mean("vortex", metric)
      regular_mean = mean("regular", metric)
      if (vortex_mean < regular_mean) {
        faster = "vortex"
        advantage = regular_mean - vortex_mean
        advantage_pct = regular_mean == 0 ? 0 : 100 * advantage / regular_mean
      } else if (regular_mean < vortex_mean) {
        faster = "regular"
        advantage = vortex_mean - regular_mean
        advantage_pct = vortex_mean == 0 ? 0 : 100 * advantage / vortex_mean
      } else {
        faster = "tie"
        advantage = 0
        advantage_pct = 0
      }
      printf "%-10s %12.3f %12.3f %10s %13.3f %11.1f%%\n", metric, vortex_mean, regular_mean, faster, advantage, advantage_pct
    }
    print ""
    print "Notes:"
    print "  - lower times are better."
    print "  - total is the query total reported by examples/sum_perf; it excludes write time."
    print "  - write is shown only to expose data-generation noise before the query starts."
    print "  - advantage % is the faster mode'\''s saving relative to the slower mode."
  }
' "$data"
