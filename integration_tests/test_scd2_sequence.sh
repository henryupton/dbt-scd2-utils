#!/bin/bash

# SCD2 Sequential Testing Script
#
# This script performs sequential dbt runs to test SCD2 incremental logic
# with different variable configurations.
#
# Usage:
#   ./test_scd2_sequence.sh [start_num] [end_num] [model_name]
#   ./test_scd2_sequence.sh 1 3 customers_scd2

# Configuration
DEFAULT_START=1
DEFAULT_END=6
DEFAULT_MODEL="customers_scd2"
DBT_PROJECT_DIR="."

# Variable configurations to test
# Format: JSON object with config name as key and vars as value
# These will be merged with base config at runtime
VARIABLE_CONFIGS='{
  "update_all_true": {
    "dbt_scd2_utils": {
      "update_all_previous_records": true
    }
  },
  "update_all_false": {
    "dbt_scd2_utils": {
      "update_all_previous_records": false
    }
  }
}'

# Base configuration that will be merged with each test config
BASE_CONFIG='{
  "dbt_scd2_utils": {
    "is_current_column": "_is_current",
    "valid_from_column": "_valid_from",
    "valid_to_column": "_valid_to",
    "updated_at_column": "_updated_at",
    "change_type_column": "_change_type",
    "created_at_column": "_created_at"
  }
}'

# Initialize results array
RESULTS=()

# Parse arguments
START_NUM=${1:-$DEFAULT_START}
END_NUM=${2:-$DEFAULT_END}
TARGET_MODEL=${3:-$DEFAULT_MODEL}

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to run dbt command
run_dbt() {
    local cmd="$1"
    log_info "Running: dbt $cmd"
    (cd "$DBT_PROJECT_DIR" && eval "dbt $cmd")
}

# Function to display current table state
display_table_state() {
    local iteration=$1
    local model_name=$2

    log_info "📊 Table state after iteration $iteration:"
    echo "============================================================"

    local show_query="select * from {{ ref('$model_name') }} order by customer_id, _valid_from"

    if (cd "$DBT_PROJECT_DIR" && dbt show --inline "$show_query" --vars "{iteration: $iteration}"); then
        log_success "Table displayed"
    else
        log_warning "Failed to display table state"
    fi

    echo "============================================================"
}

# Function to convert JSON to YAML format recursively
json_to_yaml() {
    local json=$1
    local indent=${2:-0}

    echo "$json" | python3 -c "
import sys, json, yaml
try:
    data = json.load(sys.stdin)
    # Output as flow-style YAML (inline) which dbt accepts
    print(yaml.dump(data, default_flow_style=True, width=1000).strip())
except:
    sys.exit(1)
"
}

# Function to merge base config, test config, and iteration using jq
merge_vars_json() {
    local iteration=$1
    local test_config=$2

    # Use jq to deep merge base config with test config, then add iteration
    local merged_json=$(echo "$BASE_CONFIG" | jq --argjson test "$test_config" --arg iter "$iteration" '
        . as $base |
        $test |
        . as $merged |
        ($base * $merged) + {iteration: ($iter | tonumber)}
    ')

    # Convert to YAML format that dbt accepts
    json_to_yaml "$merged_json"
}

# Function to run a single iteration
run_iteration() {
    local iteration=$1
    local config_name=$2
    local config_json=$3

    log_info "🔄 Iteration $iteration"

    # Merge iteration with config vars
    local vars_json=$(merge_vars_json "$iteration" "$config_json")

    # Run dbt for this iteration
    if run_dbt " build --select $TARGET_MODEL+ --vars \"$vars_json\""; then
        RESULTS+=("$config_name,$iteration,SUCCESS")
        log_success "✅ Iteration $iteration completed"

        # Display current table state
        display_table_state "$iteration" "$TARGET_MODEL"
    else
        RESULTS+=("$config_name,$iteration,FAILED")
        log_error "❌ Iteration $iteration failed"
        return 1
    fi
}

# Function to run test suite for a specific configuration
run_config_test_suite() {
    local config_name=$1
    local config_json=$2

    log_info "════════════════════════════════════════"
    log_info "📋 Testing Configuration: $config_name"
    log_info "════════════════════════════════════════"
    echo

    # Clean up target model first
    log_info "Cleaning target model with full refresh..."
    local init_vars_json=$(merge_vars_json "1" "$config_json")
    if run_dbt "build --select $TARGET_MODEL --full-refresh --vars \"$init_vars_json\""; then
        log_success "Target model cleaned"
        echo
    else
        log_warning "Failed to clean target model - continuing anyway"
        echo
    fi

    # Run iterations for this configuration
    for i in $(seq $START_NUM $END_NUM); do
        run_iteration "$i" "$config_name" "$config_json"
        echo
    done
}

# Function to print summary
print_summary() {
    log_info "📊 SUMMARY"
    echo "=========================================="

    local total_tests=${#RESULTS[@]}
    local passed_tests=0

    for result in "${RESULTS[@]}"; do
        if [[ "$result" == *",SUCCESS" ]]; then
            ((passed_tests++))
        fi
    done

    local failed_tests=$((total_tests - passed_tests))

    log_info "Total test runs: $total_tests"
    log_success "Passed: $passed_tests"

    if [[ $failed_tests -gt 0 ]]; then
        log_error "Failed: $failed_tests"
    else
        log_success "Failed: $failed_tests"
    fi

    echo
    printf "%-30s %-10s %-10s\n" "Configuration" "Iteration" "Status"
    printf "%-30s %-10s %-10s\n" "-----------------------------" "--------" "------"

    for result in "${RESULTS[@]}"; do
        IFS=',' read -r config iteration status <<< "$result"
        printf "%-30s %-10s %-10s\n" "$config" "$iteration" "$status"
    done

    if [[ $failed_tests -eq 0 ]]; then
        echo
        log_success "🎉 All tests passed!"
        return 0
    else
        echo
        log_error "⚠️  $failed_tests test(s) failed"
        return 1
    fi
}

# Main execution
main() {
    # Show usage if help requested
    if [[ "$1" == "-h" || "$1" == "--help" ]]; then
        echo "Usage: $0 [start_num] [end_num] [model_name]"
        echo
        echo "Arguments:"
        echo "  start_num     Starting iteration number (default: $DEFAULT_START)"
        echo "  end_num       Ending iteration number (default: $DEFAULT_END)"
        echo "  model_name    Target SCD2 model name (default: $DEFAULT_MODEL)"
        echo
        echo "Examples:"
        echo "  $0                    # Test iterations 1-5"
        echo "  $0 1 3                # Test iterations 1-3"
        echo "  $0 1 3 my_scd2_model  # Test iterations 1-3 with custom model"
        exit 0
    fi

    # Validate arguments
    if ! [[ "$START_NUM" =~ ^[0-9]+$ ]] || ! [[ "$END_NUM" =~ ^[0-9]+$ ]]; then
        log_error "Start and end numbers must be integers"
        exit 1
    fi

    if [[ $START_NUM -gt $END_NUM ]]; then
        log_error "Start number must be less than or equal to end number"
        exit 1
    fi

    # Check if dbt project directory exists
    if [[ ! -d "$DBT_PROJECT_DIR" ]]; then
        log_error "dbt project directory '$DBT_PROJECT_DIR' not found"
        exit 1
    fi

    # Check for jq
    if ! command -v jq &> /dev/null; then
        log_error "jq is required but not installed. Please install jq to continue."
        exit 1
    fi

    # Get count of configurations
    local config_count=$(echo "$VARIABLE_CONFIGS" | jq 'length')

    # Setup
    log_info "🚀 Starting SCD2 Sequential Testing"
    log_info "Iterations: $START_NUM to $END_NUM"
    log_info "Target model: $TARGET_MODEL"
    log_info "Variable configurations: $config_count"
    log_info "dbt project: $DBT_PROJECT_DIR"
    echo

    # Full refresh seeds first
    log_info "Refreshing all seeds..."
    if run_dbt "seed --full-refresh"; then
        log_success "Seeds refreshed"
        echo
    else
        log_warning "Failed to refresh seeds - continuing anyway"
        echo
    fi

    # Run test suite for each configuration
    echo "$VARIABLE_CONFIGS" | jq -r 'keys[]' | while read -r config_name; do
        local config_vars=$(echo "$VARIABLE_CONFIGS" | jq -c ".[\"$config_name\"]")
        run_config_test_suite "$config_name" "$config_vars"
        echo
    done

    # Print summary
    print_summary
    exit_code=$?

    # Final message
    if [[ $exit_code -eq 0 ]]; then
        log_success "🎉 All SCD2 tests completed successfully!"
    else
        log_error "⚠️  SCD2 testing completed with failures"
    fi

    exit $exit_code
}

# Run main function
main "$@"