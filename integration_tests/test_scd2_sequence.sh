#!/bin/bash

# SCD2 Sequential Testing Script
# 
# This script performs sequential dbt runs to test SCD2 incremental logic.
#
# Usage:
#   ./test_scd2_sequence.sh [start_num] [end_num] [model_name]
#   ./test_scd2_sequence.sh 1 3 customers_scd2

# Configuration
DEFAULT_START=1
DEFAULT_END=5
DEFAULT_MODEL="customers_scd2"
DBT_PROJECT_DIR="."

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

# Function to run a single iteration
run_iteration() {
    local iteration=$1

    log_info "🔄 Iteration $iteration"

    # Run dbt for this iteration
    if run_dbt "build --select $TARGET_MODEL+ --vars \"{iteration: $iteration}\""; then
        RESULTS+=("$iteration,SUCCESS")
        log_success "✅ Iteration $iteration completed"

        # Display current table state
        display_table_state "$iteration" "$TARGET_MODEL"
    else
        RESULTS+=("$iteration,FAILED")
        log_error "❌ Iteration $iteration failed"
        return 1
    fi
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

    log_info "Total iterations: $total_tests"
    log_success "Passed: $passed_tests"

    if [[ $failed_tests -gt 0 ]]; then
        log_error "Failed: $failed_tests"
    else
        log_success "Failed: $failed_tests"
    fi

    echo
    printf "%-10s %-10s\n" "Iteration" "Status"
    printf "%-10s %-10s\n" "--------" "------"

    for result in "${RESULTS[@]}"; do
        IFS=',' read -r iteration status <<< "$result"
        printf "%-10s %-10s\n" "$iteration" "$status"
    done

    if [[ $failed_tests -eq 0 ]]; then
        echo
        log_success "🎉 All iterations passed!"
        return 0
    else
        echo
        log_error "⚠️  $failed_tests iteration(s) failed"
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

    # Setup
    log_info "🚀 Starting SCD2 Sequential Testing"
    log_info "Iterations: $START_NUM to $END_NUM"
    log_info "Target model: $TARGET_MODEL"
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

    # Clean up target model first
    log_info "Cleaning target model with full refresh..."
    if run_dbt "build --select $TARGET_MODEL --full-refresh --vars \"{iteration: 1}\""; then
        log_success "Target model cleaned"
        echo
    else
        log_warning "Failed to clean target model - continuing anyway"
        echo
    fi

    # Run iterations
    for i in $(seq $START_NUM $END_NUM); do
        run_iteration "$i"
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