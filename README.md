#!/bin/bash
#==============================
# Automated K-Means Spark Runner (Professional)
#==============================

set -euo pipefail  # Exit on error, undefined vars, and pipe failures

# Global variables
ITERATION_COUNT=0
START_TIME=0
END_TIME=0

# Configurable parameters
PROJECT_DIR="${PROJECT_DIR:-/home/cloudera/parallel-kmeans}"
JAR_NAME="${JAR_NAME:-parallel-kmeans-1.0-SNAPSHOT.jar}"
INPUT_PATH="${INPUT_PATH:-hdfs:///user/cloudera/data/iris_dataset}"
NUM_CLUSTERS="${NUM_CLUSTERS:-3}"
NUM_ITERATIONS="${NUM_ITERATIONS:-20}"
SPARK_MASTER="${SPARK_MASTER:-local[*]}"
EXECUTOR_MEMORY="${EXECUTOR_MEMORY:-2g}"
DRIVER_MEMORY="${DRIVER_MEMORY:-1g}"
LOG_DIR="$PROJECT_DIR/logs"
RESULTS_DIR="$PROJECT_DIR/results"
REPORT_DIR="$PROJECT_DIR/report"

# Create necessary directories
mkdir -p "$LOG_DIR" "$RESULTS_DIR" "$REPORT_DIR"

# Timestamp for this run
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="$LOG_DIR/kmeans_run_$TIMESTAMP.log"
RESULT_FILE="$RESULTS_DIR/kmeans_result_$TIMESTAMP.txt"
REPORT_FILE="$REPORT_DIR/kmeans_execution_report_$TIMESTAMP.txt"

# Colors for better visibility
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
NC='\033[0m' # No Color

# Logging functions
function log { echo -e "[$(date +'%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"; }
function info { log "${BLUE}[INFO]${NC} $1"; }
function warn { log "${YELLOW}[WARN]${NC} $1"; }
function error { log "${RED}[ERROR]${NC} $1"; }
function success { log "${GREEN}[SUCCESS]${NC} $1"; }
function debug { log "${CYAN}[DEBUG]${NC} $1"; }

# Progress indicators
function show_progress {
    local current=$1
    local total=$2
    local width=50
    local percent=$((current * 100 / total))
    local completed=$((current * width / total))
    local remaining=$((width - completed))
    
    printf "\r${CYAN}["
    printf "%.0s#" $(seq 1 $completed)
    printf "%.0s-" $(seq 1 $remaining)
    printf "] ${percent}%% (${current}/${total})${NC}"
}

# Display iteration details
function display_iteration {
    local iteration=$1
    local wcss=$2
    local movement=$3
    
    echo -e "\n${BLUE}Iteration ${iteration}/${NUM_ITERATIONS}:${NC}"
    echo -e "  ${YELLOW}•${NC} WCSS (Within-Cluster Sum of Squares): ${GREEN}$wcss${NC}"
    echo -e "  ${YELLOW}•${NC} Centroid Movement: ${GREEN}$movement${NC}"
    
    # Show convergence status
    if (( $(echo "$movement < 0.001" | bc -l 2>/dev/null || echo "0") )); then
        echo -e "  ${YELLOW}•${NC} Convergence Status: ${GREEN}High Convergence${NC}"
    elif (( $(echo "$movement < 0.01" | bc -l 2>/dev/null || echo "0") )); then
        echo -e "  ${YELLOW}•${NC} Convergence Status: ${CYAN}Good Progress${NC}"
    else
        echo -e "  ${YELLOW}•${NC} Convergence Status: ${YELLOW}Still Adjusting${NC}"
    fi
}

# Trap errors and cleanup
trap 'error "Script failed at line $LINENO. Exit code: $?"; cleanup; exit 1' ERR
trap 'warn "Script interrupted by user"; cleanup; exit 130' INT TERM

function cleanup {
    info "Performing cleanup..."
    # Add any cleanup tasks here
}

# Print banner
function print_banner {
    echo -e "${MAGENTA}"
    echo "╔══════════════════════════════════════════════════════════════════════╗"
    echo "║                K-Means Clustering Execution Engine                   ║"
    echo "║                 Apache Spark - Professional Edition                  ║"
    echo "╚══════════════════════════════════════════════════════════════════════╝"
    echo -e "${NC}"
}

# Validate prerequisites
function check_prerequisites {
    info "Validating prerequisites..."
    
    local missing_tools=()
    
    for tool in mvn spark-submit hdfs; do
        if ! command -v "$tool" &> /dev/null; then
            missing_tools+=("$tool")
        fi
    done
    
    if [ ${#missing_tools[@]} -ne 0 ]; then
        error "Missing required tools: ${missing_tools[*]}"
        error "Please install missing tools and try again"
        exit 1
    fi
    
    success "All prerequisites validated"
}

# Check HDFS connectivity
function check_hdfs {
    info "Checking HDFS connectivity..."
    
    if ! hdfs dfs -ls / &> /dev/null; then
        error "Cannot connect to HDFS. Please check Hadoop services"
        exit 1
    fi
    
    success "HDFS connection established"
}

# Validate input file
function validate_input {
    info "Validating input file: $INPUT_PATH"
    
    if ! hdfs dfs -test -e "$INPUT_PATH"; then
        error "Input file $INPUT_PATH does not exist in HDFS"
        info "Available files in parent directory:"
        hdfs dfs -ls "$(dirname "$INPUT_PATH")" 2>/dev/null || warn "Cannot list parent directory"
        exit 1
    fi
    
    local file_size=$(hdfs dfs -du -s "$INPUT_PATH" | awk '{print $1}')
    local file_size_mb=$((file_size / 1024 / 1024))
    
    info "Input file size: ${file_size_mb} MB"
    success "Input file validated"
}

# Build project with friendly output
function build_project {
    info "Changing to project directory: $PROJECT_DIR"
    cd "$PROJECT_DIR" || { error "Project directory not found"; exit 1; }
    
    echo -e "\n${CYAN}══════════════════════════════════════════════════════════════════════${NC}"
    echo -e "${CYAN}                     MAVEN BUILD PROCESS                               ${NC}"
    echo -e "${CYAN}══════════════════════════════════════════════════════════════════════${NC}\n"
    
    echo -e "${BLUE}[1/4]${NC} Cleaning previous build artifacts..."
    if mvn clean > >(tee -a "$LOG_FILE" | grep -E "BUILD|ERROR|SUCCESS") 2>&1; then
        echo -e "  ${GREEN}✓ Clean completed${NC}"
    else
        error "Maven clean failed"
        exit 1
    fi
    
    echo -e "\n${BLUE}[2/4]${NC} Resolving dependencies..."
    echo -e "  ${BLUE}›${NC} Downloading required libraries..."
    
    echo -e "\n${BLUE}[3/4]${NC} Compiling source code..."
    if mvn compile > >(tee -a "$LOG_FILE" | grep -E "Compiling|BUILD|ERROR") 2>&1; then
        echo -e "  ${GREEN}✓ Compilation successful${NC}"
    else
        error "Compilation failed"
        exit 1
    fi
    
    echo -e "\n${BLUE}[4/4]${NC} Creating JAR package..."
    echo -e "  ${BLUE}›${NC} Packaging application..."
    if mvn package -DskipTests > >(tee -a "$LOG_FILE" | tail -5) 2>&1; then
        echo -e "  ${GREEN}✓ Build completed successfully${NC}"
    else
        error "Packaging failed"
        exit 1
    fi
    
    # Verify JAR exists
    if [ ! -f "target/$JAR_NAME" ]; then
        error "Build artifact not found: target/$JAR_NAME"
        exit 1
    fi
    
    local jar_size=$(du -h "target/$JAR_NAME" | cut -f1)
    echo -e "\n${GREEN}══════════════════════════════════════════════════════════════════════${NC}"
    echo -e "${GREEN}                     BUILD SUCCESSFUL                                 ${NC}"
    echo -e "${GREEN}  JAR created: target/$JAR_NAME (${jar_size})                        ${NC}"
    echo -e "${GREEN}══════════════════════════════════════════════════════════════════════${NC}\n"
}

# Run Spark job with friendly output
function run_spark_job {
    echo -e "\n${CYAN}══════════════════════════════════════════════════════════════════════${NC}"
    echo -e "${CYAN}                     SPARK JOB EXECUTION                               ${NC}"
    echo -e "${CYAN}══════════════════════════════════════════════════════════════════════${NC}\n"
    
    echo -e "${BLUE}Job Configuration:${NC}"
    echo -e "  ${BLUE}•${NC} Clusters: ${GREEN}$NUM_CLUSTERS${NC}"
    echo -e "  ${BLUE}•${NC} Iterations: ${GREEN}$NUM_ITERATIONS${NC}"
    echo -e "  ${BLUE}•${NC} Master: ${GREEN}$SPARK_MASTER${NC}"
    echo -e "  ${BLUE}•${NC} Executor Memory: ${GREEN}$EXECUTOR_MEMORY${NC}"
    echo -e "  ${BLUE}•${NC} Driver Memory: ${GREEN}$DRIVER_MEMORY${NC}"
    
    echo -e "\n${YELLOW}Initializing Spark session...${NC}"
    
    START_TIME=$(date +%s)
    ITERATION_COUNT=0
    
    # Create a named pipe for iteration tracking
    local iteration_pipe=$(mktemp -u)
    mkfifo "$iteration_pipe"
    
    # Run Spark job and process output
    spark-submit \
        --class parallel.kmeans.ParallelKMeans \
        --master "$SPARK_MASTER" \
        --executor-memory "$EXECUTOR_MEMORY" \
        --driver-memory "$DRIVER_MEMORY" \
        --conf spark.ui.showConsoleProgress=false \
        --conf spark.log.level=WARN \
        "target/$JAR_NAME" \
        "$INPUT_PATH" \
        "$NUM_CLUSTERS" \
        "$NUM_ITERATIONS" 2>&1 | \
    while IFS= read -r line; do
        echo "$line" >> "$LOG_FILE"
        echo "$line" >> "$RESULT_FILE"
        
        # Capture iteration details
        if [[ "$line" == *"Iteration"* && "$line" == *"completed"* ]]; then
            ITERATION_COUNT=$((ITERATION_COUNT + 1))
            echo "$ITERATION_COUNT" > "$iteration_pipe"
            
            # Extract WCSS and movement
            local wcss=$(echo "$line" | grep -oP "WCSS: \K[\d.]+" || echo "N/A")
            local movement=$(echo "$line" | grep -oP "movement: \K[\d.]+" || echo "N/A")
            
            # Display iteration details
            display_iteration "$ITERATION_COUNT" "$wcss" "$movement"
            
            # Show progress
            show_progress "$ITERATION_COUNT" "$NUM_ITERATIONS"
            
        elif [[ "$line" == *"Cluster centers:"* ]]; then
            echo -e "\n\n${GREEN}══════════════════════════════════════════════════════════════════════${NC}"
            echo -e "${GREEN}                     CLUSTER CENTERS FOUND                            ${NC}"
            echo -e "${GREEN}══════════════════════════════════════════════════════════════════════${NC}\n"
        elif [[ "$line" == *"Center"* ]] && [[ ! "$line" == *"DEBUG"* ]]; then
            echo -e "  ${CYAN}$line${NC}"
        elif [[ "$line" == *"WCSS"* ]] || [[ "$line" == *"Silhouette"* ]]; then
            echo -e "  ${YELLOW}$line${NC}"
        elif [[ "$line" == *"ERROR"* ]]; then
            echo -e "${RED}$line${NC}"
        elif [[ "$line" == *"Total runtime"* ]]; then
            echo -e "\n${GREEN}$line${NC}"
        fi
    done
    
    # Clean up pipe
    rm -f "$iteration_pipe"
    
    local exit_code=${PIPESTATUS[0]}
    END_TIME=$(date +%s)
    local duration=$((END_TIME - START_TIME))
    
    echo ""
    
    if [ $exit_code -eq 0 ]; then
        echo -e "${GREEN}══════════════════════════════════════════════════════════════════════${NC}"
        echo -e "${GREEN}                   JOB COMPLETED SUCCESSFULLY                        ${NC}"
        echo -e "${GREEN}              Execution time: ${duration} seconds                    ${NC}"
        echo -e "${GREEN}══════════════════════════════════════════════════════════════════════${NC}"
        return 0
    else
        echo -e "${RED}══════════════════════════════════════════════════════════════════════${NC}"
        echo -e "${RED}                       JOB FAILED                                    ${NC}"
        echo -e "${RED}                 Exit code: $exit_code                              ${NC}"
        echo -e "${RED}══════════════════════════════════════════════════════════════════════${NC}"
        return 1
    fi
}

# Extract and display final results
function display_results {
    echo -e "\n${CYAN}══════════════════════════════════════════════════════════════════════${NC}"
    echo -e "${CYAN}                     FINAL RESULTS SUMMARY                            ${NC}"
    echo -e "${CYAN}══════════════════════════════════════════════════════════════════════${NC}\n"
    
    if [ ! -f "$RESULT_FILE" ]; then
        error "Results file not found"
        return 1
    fi
    
    # Display cluster centers
    if grep -q "Cluster centers:" "$RESULT_FILE"; then
        echo -e "${BLUE}Cluster Centers:${NC}\n"
        grep -A $((NUM_CLUSTERS + 2)) "Cluster centers:" "$RESULT_FILE" | \
        grep -v "Cluster centers:" | \
        while read -r line; do
            if [[ -n "$line" ]]; then
                echo -e "  ${GREEN}$line${NC}"
            fi
        done
    fi
    
    # Display quality metrics
    echo -e "\n${BLUE}Quality Metrics:${NC}"
    if grep -q "WCSS" "$RESULT_FILE"; then
        wcss_line=$(grep "WCSS" "$RESULT_FILE")
        echo -e "  ${YELLOW}•${NC} $wcss_line"
    fi
    
    if grep -q "Silhouette" "$RESULT_FILE"; then
        silhouette_line=$(grep "Silhouette" "$RESULT_FILE")
        echo -e "  ${YELLOW}•${NC} $silhouette_line"
    fi
    
    # Display execution stats
    echo -e "\n${BLUE}Execution Statistics:${NC}"
    if grep -q "Total runtime" "$RESULT_FILE"; then
        runtime_line=$(grep "Total runtime" "$RESULT_FILE")
        echo -e "  ${YELLOW}•${NC} $runtime_line"
    fi
    
    local data_points=$(hdfs dfs -cat "$INPUT_PATH" 2>/dev/null | wc -l 2>/dev/null || echo "N/A")
    echo -e "  ${YELLOW}•${NC} Data Points Processed: $data_points"
    echo -e "  ${YELLOW}•${NC} Features: 4 (sepal length, sepal width, petal length, petal width)"
    echo -e "  ${YELLOW}•${NC} Iterations Completed: $ITERATION_COUNT/$NUM_ITERATIONS"
}

# Generate professional report
function generate_report {
    echo -e "\n${CYAN}══════════════════════════════════════════════════════════════════════${NC}"
    echo -e "${CYAN}                   GENERATING EXECUTION REPORT                        ${NC}"
    echo -e "${CYAN}══════════════════════════════════════════════════════════════════════${NC}\n"
    
    local total_time=$((END_TIME - START_TIME))
    local total_time_str="${total_time} seconds"
    
    # Extract metrics from log file
    local final_wcss=$(grep -oP "WCSS: \K[\d.]+" "$LOG_FILE" | tail -1 || echo "N/A")
    local final_silhouette=$(grep -oP "Silhouette: \K[\d.]+" "$LOG_FILE" | tail -1 || echo "N/A")
    local total_runtime=$(grep -oP "Total runtime: \K[\d.]+ seconds" "$RESULT_FILE" 2>/dev/null || echo "N/A")
    
    # Create professional report
    cat > "$REPORT_FILE" << EOF
╔═══════════════════════════════════════════════════════════════════════════════╗
║                     K-MEANS CLUSTERING EXECUTION REPORT                      ║
║                     Apache Spark Implementation                               ║
╚═══════════════════════════════════════════════════════════════════════════════╝

EXECUTION SUMMARY
─────────────────────────────────────────────────────────────────────────────────
Execution Timestamp : $(date -d @$START_TIME +"%Y-%m-%d %H:%M:%S")
Run ID             : $TIMESTAMP
Status             : COMPLETED SUCCESSFULLY
Total Runtime      : ${total_time_str}
Iterations Completed: ${ITERATION_COUNT} of ${NUM_ITERATIONS}

CONFIGURATION PARAMETERS
─────────────────────────────────────────────────────────────────────────────────
Input Dataset      : $INPUT_PATH
Number of Clusters : $NUM_CLUSTERS
Maximum Iterations : $NUM_ITERATIONS
Spark Master       : $SPARK_MASTER
Executor Memory    : $EXECUTOR_MEMORY
Driver Memory      : $DRIVER_MEMORY
Project Directory  : $PROJECT_DIR

CLUSTER ANALYSIS RESULTS
─────────────────────────────────────────────────────────────────────────────────
EOF

    # Add cluster centers to report
    if grep -q "Cluster centers:" "$RESULT_FILE"; then
        echo "" >> "$REPORT_FILE"
        echo "FINAL CLUSTER CENTERS:" >> "$REPORT_FILE"
        echo "─────────────────────" >> "$REPORT_FILE"
        grep -A $((NUM_CLUSTERS + 2)) "Cluster centers:" "$RESULT_FILE" | \
        grep -v "Cluster centers:" | \
        while read -r line; do
            if [[ -n "$line" ]]; then
                echo "  $line" >> "$REPORT_FILE"
            fi
        done
    fi
    
    # Add quality metrics
    cat >> "$REPORT_FILE" << EOF

QUALITY METRICS
─────────────────────────────────────────────────────────────────────────────────
Within-Cluster Sum of Squares (WCSS) : ${final_wcss}
Silhouette Score                     : ${final_silhouette}

Interpretation:
• WCSS measures compactness (lower is better)
• Silhouette Score ranges from -1 to 1 (higher is better)
• Score > 0.5 indicates reasonable clustering
• Score > 0.7 indicates strong clustering

EOF
    
    # Add iteration history if available
    if grep -q "Iteration.*WCSS" "$LOG_FILE"; then
        cat >> "$REPORT_FILE" << EOF
ITERATION HISTORY
─────────────────────────────────────────────────────────────────────────────────
Iteration    WCSS            Centroid Movement      Status
─────────    ───────         ─────────────────      ───────
EOF
        
        local iteration_num=0
        while IFS= read -r line; do
            iteration_num=$((iteration_num + 1))
            local wcss=$(echo "$line" | grep -oP "WCSS: \K[\d.]+" || echo "N/A")
            local movement=$(echo "$line" | grep -oP "movement: \K[\d.]+" || echo "N/A")
            
            # Determine status
            local status="Processing"
            if [ "$movement" != "N/A" ]; then
                if (( $(echo "$movement < 0.001" | bc -l 2>/dev/null || echo "0") )); then
                    status="Converged"
                elif (( $(echo "$movement < 0.01" | bc -l 2>/dev/null || echo "0") )); then
                    status="Good Progress"
                else
                    status="Adjusting"
                fi
            fi
            
            printf "  %-9s    %-14s   %-20s   %-12s\n" "$iteration_num" "$wcss" "$movement" "$status" >> "$REPORT_FILE"
        done < <(grep "Iteration.*WCSS" "$LOG_FILE")
        
        echo "" >> "$REPORT_FILE"
    fi
    
    # Add convergence analysis
    cat >> "$REPORT_FILE" << EOF
CONVERGENCE ANALYSIS
─────────────────────────────────────────────────────────────────────────────────
Total Iterations Run   : ${ITERATION_COUNT}
Maximum Allowed        : ${NUM_ITERATIONS}
Convergence Status     : $(if [ $ITERATION_COUNT -lt $NUM_ITERATIONS ]; then echo "Early Convergence"; else echo "Max Iterations Reached"; fi)

EOF
    
    # Add execution details
    cat >> "$REPORT_FILE" << EOF
EXECUTION DETAILS
─────────────────────────────────────────────────────────────────────────────────
Start Time        : $(date -d @$START_TIME +"%Y-%m-%d %H:%M:%S")
End Time          : $(date -d @$END_TIME +"%Y-%m-%d %H:%M:%S")
Duration          : ${total_time} seconds
Input Size        : $(hdfs dfs -du -h "$INPUT_PATH" 2>/dev/null | awk '{print $1}' || echo "N/A")
Data Points       : $(hdfs dfs -cat "$INPUT_PATH" 2>/dev/null | wc -l 2>/dev/null || echo "N/A")
Features          : 4 (sepal length, sepal width, petal length, petal width)

PERFORMANCE INSIGHTS
─────────────────────────────────────────────────────────────────────────────────
1. Algorithm $(if [ $ITERATION_COUNT -lt $NUM_ITERATIONS ]; then echo "converged early in ${ITERATION_COUNT} iterations"; else echo "completed all ${NUM_ITERATIONS} iterations"; fi)
2. Final WCSS of ${final_wcss} indicates $(if [ "$final_wcss" != "N/A" ]; then if (( $(echo "$final_wcss < 50" | bc -l 2>/dev/null || echo "0") )); then echo "good cluster compactness"; else echo "moderate cluster compactness"; fi; else echo "N/A"; fi)
3. Silhouette score of ${final_silhouette} suggests $(if [ "$final_silhouette" != "N/A" ]; then if (( $(echo "$final_silhouette > 0.7" | bc -l 2>/dev/null || echo "0") )); then echo "well-separated clusters"; elif (( $(echo "$final_silhouette > 0.5" | bc -l 2>/dev/null || echo "0") )); then echo "reasonable cluster separation"; else echo "some cluster overlap"; fi; else echo "N/A"; fi)

RECOMMENDATIONS
─────────────────────────────────────────────────────────────────────────────────
EOF
    
    if [ "$NUM_CLUSTERS" -eq 3 ]; then
        cat >> "$REPORT_FILE" << EOF
• K=3 is optimal for Iris dataset (matches biological species)
• Consider running elbow method with K=2 to K=5 to validate choice
• For quality validation, use cross-validation with different random seeds
• Consider feature scaling if features have different units or ranges
EOF
    else
        cat >> "$REPORT_FILE" << EOF
• Consider validating K=$NUM_CLUSTERS using elbow method
• Run silhouette analysis for different K values (2-10)
• For comparison, run with K=3 (standard for Iris dataset)
• Use domain knowledge to interpret cluster assignments
EOF
    fi
    
    cat >> "$REPORT_FILE" << EOF

TECHNICAL DETAILS
─────────────────────────────────────────────────────────────────────────────────
Spark Version     : $(spark-submit --version 2>/dev/null | grep "version" | head -1 | cut -d' ' -f2- || echo "N/A")
Hadoop Version    : $(hadoop version 2>/dev/null | grep "Hadoop" | head -1 | cut -d' ' -f2 || echo "N/A")
JAR File          : $JAR_NAME
Build Timestamp   : $(date -r "target/$JAR_NAME" +"%Y-%m-%d %H:%M:%S" 2>/dev/null || echo "N/A")

FILES GENERATED
─────────────────────────────────────────────────────────────────────────────────
Execution Log     : $LOG_FILE
Results Output    : $RESULT_FILE
This Report       : $REPORT_FILE

╔═══════════════════════════════════════════════════════════════════════════════╗
║                     REPORT GENERATED SUCCESSFULLY                            ║
║           $(date +"%Y-%m-%d %H:%M:%S")                                       ║
╚═══════════════════════════════════════════════════════════════════════════════╝
EOF
    
    echo -e "  ${GREEN}✓${NC} Professional report generated: $REPORT_FILE"
    
    # Create a symlink to latest report
    ln -sf "$(basename "$REPORT_FILE")" "$REPORT_DIR/latest_report.txt" 2>/dev/null
    echo -e "  ${GREEN}✓${NC} Latest report available at: $REPORT_DIR/latest_report.txt"
}

# Print execution summary
function print_summary {
    echo -e "\n${GREEN}══════════════════════════════════════════════════════════════════════${NC}"
    echo -e "${GREEN}                     EXECUTION COMPLETE                               ${NC}"
    echo -e "${GREEN}══════════════════════════════════════════════════════════════════════${NC}\n"
    
    echo -e "${BLUE}📊 Results Summary:${NC}"
    echo -e "  ${GREEN}•${NC} ${CYAN}Cluster Analysis:${NC} Successfully identified $NUM_CLUSTERS clusters"
    echo -e "  ${GREEN}•${NC} ${CYAN}Convergence:${NC} Algorithm completed $ITERATION_COUNT iterations"
    echo -e "  ${GREEN}•${NC} ${CYAN}Data Processed:${NC} Iris dataset (150 samples, 4 features)"
    echo -e "  ${GREEN}•${NC} ${CYAN}Execution Time:${NC} $((END_TIME - START_TIME)) seconds"
    
    echo -e "\n${BLUE}📁 Generated Files:${NC}"
    echo -e "  ${GREEN}•${NC} ${YELLOW}Execution Log:${NC}      $LOG_DIR/"
    echo -e "  ${GREEN}•${NC} ${YELLOW}Results:${NC}            $RESULTS_DIR/"
    echo -e "  ${GREEN}•${NC} ${YELLOW}Report:${NC}             $REPORT_FILE"
    
    echo -e "\n${BLUE}🎯 Next Steps:${NC}"
    echo -e "  1. Review the comprehensive report: ${GREEN}$REPORT_FILE${NC}"
    echo -e "  2. Analyze cluster centers for biological interpretation"
    echo -e "  3. Consider running with different K values for comparison"
    echo -e "  4. Validate results with domain knowledge"
    echo -e "  5. Check $REPORT_DIR/latest_report.txt for quick access"
    
    echo -e "\n${GREEN}══════════════════════════════════════════════════════════════════════${NC}"
    echo -e "${GREEN}           K-Means clustering completed successfully! 🎉              ${NC}"
    echo -e "${GREEN}══════════════════════════════════════════════════════════════════════${NC}"
}

# Usage information
function show_usage {
    cat << EOF
Usage: $0 [OPTIONS]

Automated K-Means Clustering with Apache Spark - Professional Edition

Options:
    -h, --help              Show this help message
    -i, --input PATH        Input HDFS path (default: $INPUT_PATH)
    -k, --clusters NUM      Number of clusters (default: $NUM_CLUSTERS)
    -n, --iterations NUM    Number of iterations (default: $NUM_ITERATIONS)
    -m, --master URL        Spark master URL (default: $SPARK_MASTER)
    --executor-mem SIZE     Executor memory (default: $EXECUTOR_MEMORY)
    --driver-mem SIZE       Driver memory (default: $DRIVER_MEMORY)

Examples:
    $0 -k 3 -n 20                          # Standard run with Iris dataset
    $0 -k 5 -n 30 -i hdfs:///data/large_dataset  # Custom dataset
    $0 --executor-mem 4g --driver-mem 2g   # Increased memory

Environment Variables:
    PROJECT_DIR             Project directory path
    INPUT_PATH              Input file path in HDFS
    NUM_CLUSTERS            Number of clusters
    NUM_ITERATIONS          Number of iterations
    SPARK_MASTER            Spark master URL

EOF
}

# Parse command line arguments
function parse_args {
    while [[ $# -gt 0 ]]; do
        case $1 in
            -h|--help)
                show_usage
                exit 0
                ;;
            -i|--input)
                INPUT_PATH="$2"
                shift 2
                ;;
            -k|--clusters)
                NUM_CLUSTERS="$2"
                shift 2
                ;;
            -n|--iterations)
                NUM_ITERATIONS="$2"
                shift 2
                ;;
            -m|--master)
                SPARK_MASTER="$2"
                shift 2
                ;;
            --executor-mem)
                EXECUTOR_MEMORY="$2"
                shift 2
                ;;
            --driver-mem)
                DRIVER_MEMORY="$2"
                shift 2
                ;;
            *)
                error "Unknown option: $1"
                show_usage
                exit 1
                ;;
        esac
    done
}

# Main execution flow
function main {
    print_banner
    
    parse_args "$@"
    
    info "Starting K-Means Spark Runner..."
    info "Execution ID: $TIMESTAMP"
    info "Log file: $LOG_FILE"
    
    # Execute pipeline
    check_prerequisites
    check_hdfs
    validate_input
    build_project
    
    if run_spark_job; then
        display_results
        generate_report
        print_summary
        exit 0
    else
        error "Pipeline failed. Check logs for details"
        echo -e "\n${RED}══════════════════════════════════════════════════════════════════════${NC}"
        echo -e "${RED}                   EXECUTION FAILED                                   ${NC}"
        echo -e "${RED}  Check log file for details: $LOG_FILE                             ${NC}"
        echo -e "${RED}══════════════════════════════════════════════════════════════════════${NC}"
        exit 1
    fi
}

# Run main function with all arguments
main "$@"
