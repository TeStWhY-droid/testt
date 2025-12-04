#!/bin/bash
#==============================
# Professional K-Means Spark Pipeline - Enterprise Edition
# Automated Clustering with Advanced Analytics & Reporting
#==============================

set -euo pipefail

# ===========================
# CONFIGURATION
# ===========================
PROJECT_DIR="${PROJECT_DIR:-/home/cloudera/parallel-kmeans}"
JAR_NAME="${JAR_NAME:-parallel-kmeans-1.0-SNAPSHOT.jar}"
INPUT_PATH="${INPUT_PATH:-hdfs:///user/cloudera/data/iris_dataset}"
NUM_CLUSTERS="${NUM_CLUSTERS:-3}"
NUM_ITERATIONS="${NUM_ITERATIONS:-20}"
SPARK_MASTER="${SPARK_MASTER:-local[*]}"
EXECUTOR_MEMORY="${EXECUTOR_MEMORY:-2g}"
DRIVER_MEMORY="${DRIVER_MEMORY:-1g}"
EXECUTOR_CORES="${EXECUTOR_CORES:-2}"
NUM_EXECUTORS="${NUM_EXECUTORS:-2}"

# Enhanced configuration
ENABLE_DYNAMIC_ALLOCATION="${ENABLE_DYNAMIC_ALLOCATION:-true}"
AUTO_OPTIMIZE_K="${AUTO_OPTIMIZE_K:-false}"
K_RANGE="${K_RANGE:-2,3,4,5}"
ENABLE_VISUALIZATION="${ENABLE_VISUALIZATION:-true}"
ENABLE_PROFILING="${ENABLE_PROFILING:-true}"
MAX_RETRIES="${MAX_RETRIES:-3}"
HEALTH_CHECK_INTERVAL="${HEALTH_CHECK_INTERVAL:-30}"

# Directory structure
LOG_DIR="$PROJECT_DIR/logs"
RESULTS_DIR="$PROJECT_DIR/results"
REPORT_DIR="$PROJECT_DIR/reports"
METRICS_DIR="$PROJECT_DIR/metrics"
BACKUP_DIR="$PROJECT_DIR/backup"
VISUALIZATION_DIR="$PROJECT_DIR/visualizations"

# Create comprehensive directory structure
for dir in "$LOG_DIR" "$RESULTS_DIR" "$REPORT_DIR" "$METRICS_DIR" "$BACKUP_DIR" "$VISUALIZATION_DIR"; do
    mkdir -p "$dir"
done

# Timestamp for this run
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
RUN_ID="${TIMESTAMP}_$$"
LOG_FILE="$LOG_DIR/pipeline_${RUN_ID}.log"
RESULT_FILE="$RESULTS_DIR/results_${RUN_ID}.txt"
METRICS_FILE="$METRICS_DIR/metrics_${RUN_ID}.json"
FINAL_REPORT="$REPORT_DIR/K-Means_Report_${TIMESTAMP}.html"
PERF_LOG="$METRICS_DIR/performance_${RUN_ID}.log"

# ===========================
# COLORS & FORMATTING
# ===========================
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
WHITE='\033[1;37m'
BOLD='\033[1m'
DIM='\033[2m'
NC='\033[0m'

# Animation characters
SPINNER=("⠋" "⠙" "⠹" "⠸" "⠼" "⠴" "⠦" "⠧" "⠇" "⠏")
PROGRESS_CHARS=("▏" "▎" "▍" "▌" "▋" "▊" "▉" "█")

# ===========================
# LOGGING FUNCTIONS
# ===========================
function log { 
    echo -e "[$(date +'%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

function info { log "${BLUE}[INFO]${NC} $1"; }
function warn { log "${YELLOW}[WARN]${NC} $1"; }
function error { log "${RED}[ERROR]${NC} $1"; }
function success { log "${GREEN}[SUCCESS]${NC} $1"; }
function debug { log "${CYAN}[DEBUG]${NC} $1"; }
function perf { echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1" >> "$PERF_LOG"; }

# ===========================
# ANIMATION FUNCTIONS
# ===========================
function show_spinner {
    local pid=$1
    local message=$2
    local i=0
    
    echo -n "$message "
    while kill -0 $pid 2>/dev/null; do
        echo -ne "\r$message ${CYAN}${SPINNER[$i]}${NC} "
        i=$(( (i + 1) % ${#SPINNER[@]} ))
        sleep 0.1
    done
    echo -ne "\r$message ${GREEN}✓${NC}\n"
}

function progress_bar {
    local current=$1
    local total=$2
    local width=50
    local percentage=$((current * 100 / total))
    local filled=$((width * current / total))
    local empty=$((width - filled))
    
    printf "\r${CYAN}["
    printf "%${filled}s" | tr ' ' '█'
    printf "%${empty}s" | tr ' ' '░'
    printf "]${NC} ${WHITE}%3d%%${NC} ${DIM}(%d/%d)${NC}" $percentage $current $total
}

function animated_header {
    local text=$1
    local width=70
    local padding=$(( (width - ${#text}) / 2 ))
    
    echo
    echo -e "${MAGENTA}${BOLD}"
    echo "╔$(printf '═%.0s' $(seq 1 $width))╗"
    printf "║%${padding}s" ""
    echo -n "$text"
    printf "%$((width - padding - ${#text}))s║\n" ""
    echo "╚$(printf '═%.0s' $(seq 1 $width))╝"
    echo -e "${NC}"
}

# ===========================
# ERROR HANDLING
# ===========================
function cleanup {
    info "Performing cleanup operations..."
    
    # Archive logs
    if [ -f "$LOG_FILE" ]; then
        gzip -c "$LOG_FILE" > "$BACKUP_DIR/pipeline_${RUN_ID}.log.gz" 2>/dev/null || true
    fi
    
    # Clean temporary files
    rm -f /tmp/calc_accuracy_*.py /tmp/spark_temp_* 2>/dev/null || true
}

trap 'error "Pipeline failed at line $LINENO. Exit code: $?"; cleanup; exit 1' ERR
trap 'warn "Pipeline interrupted by user"; cleanup; exit 130' INT TERM

# ===========================
# SYSTEM VALIDATION
# ===========================
function print_banner {
    clear
    echo -e "${MAGENTA}${BOLD}"
    cat << "EOF"
╔══════════════════════════════════════════════════════════════════════╗
║                                                                      ║
║     ██╗  ██╗      ███╗   ███╗███████╗ █████╗ ███╗   ██╗███████╗    ║
║     ██║ ██╔╝      ████╗ ████║██╔════╝██╔══██╗████╗  ██║██╔════╝    ║
║     █████╔╝ █████╗██╔████╔██║█████╗  ███████║██╔██╗ ██║███████╗    ║
║     ██╔═██╗ ╚════╝██║╚██╔╝██║██╔══╝  ██╔══██║██║╚██╗██║╚════██║    ║
║     ██║  ██╗      ██║ ╚═╝ ██║███████╗██║  ██║██║ ╚████║███████║    ║
║     ╚═╝  ╚═╝      ╚═╝     ╚═╝╚══════╝╚═╝  ╚═╝╚═╝  ╚═══╝╚══════╝    ║
║                                                                      ║
║           Enterprise Clustering Pipeline with Apache Spark          ║
║              Professional Analytics & Automated Reporting            ║
║                                                                      ║
╚══════════════════════════════════════════════════════════════════════╝
EOF
    echo -e "${NC}"
    sleep 0.5
}

function check_system_resources {
    animated_header "SYSTEM RESOURCE VALIDATION"
    
    local issues=0
    
    # Check available memory
    local available_mem=$(free -m | awk '/^Mem:/{print $7}')
    local required_mem=4096
    
    if [ "$available_mem" -lt "$required_mem" ]; then
        warn "Low memory: ${available_mem}MB available (recommended: ${required_mem}MB)"
        ((issues++))
    else
        success "Memory: ${GREEN}${available_mem}MB available${NC}"
    fi
    
    # Check disk space
    local available_disk=$(df -BG "$PROJECT_DIR" | awk 'NR==2 {print $4}' | sed 's/G//')
    local required_disk=5
    
    if [ "$available_disk" -lt "$required_disk" ]; then
        warn "Low disk space: ${available_disk}GB available (recommended: ${required_disk}GB)"
        ((issues++))
    else
        success "Disk Space: ${GREEN}${available_disk}GB available${NC}"
    fi
    
    # Check CPU cores
    local cpu_cores=$(nproc)
    success "CPU Cores: ${GREEN}${cpu_cores} available${NC}"
    
    if [ $issues -gt 0 ]; then
        warn "System resource warnings detected. Continuing with caution..."
    fi
    
    perf "SYSTEM_CHECK available_mem=${available_mem}MB available_disk=${available_disk}GB cpu_cores=${cpu_cores}"
}

function check_prerequisites {
    animated_header "PREREQUISITES VALIDATION"
    
    local tools=("mvn" "spark-submit" "hdfs" "python" "java")
    local missing_tools=()
    local total=${#tools[@]}
    local current=0
    
    for tool in "${tools[@]}"; do
        current=$((current + 1))
        progress_bar $current $total
        sleep 0.1
        
        if ! command -v "$tool" &> /dev/null; then
            missing_tools+=("$tool")
        fi
    done
    echo
    
    if [ ${#missing_tools[@]} -ne 0 ]; then
        error "Missing required tools: ${missing_tools[*]}"
        error "Please install missing dependencies and try again"
        exit 1
    fi
    
    # Check Java version
    local java_version=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}' | cut -d'.' -f1)
    if [ "$java_version" -ge 8 ]; then
        success "Java version: ${GREEN}${java_version}${NC}"
    else
        error "Java 8 or higher required"
        exit 1
    fi
    
    success "All prerequisites validated ✓"
    perf "PREREQUISITES_CHECK status=passed"
}

function check_hdfs_health {
    animated_header "HDFS HEALTH CHECK"
    
    local retry_count=0
    local max_retries=3
    
    while [ $retry_count -lt $max_retries ]; do
        if hdfs dfsadmin -report &> /dev/null; then
            local hdfs_capacity=$(hdfs dfsadmin -report 2>/dev/null | grep "DFS Used%" | awk '{print $3}')
            success "HDFS connection established ✓"
            info "HDFS Usage: ${CYAN}${hdfs_capacity}${NC}"
            
            # Check HDFS health
            local dead_nodes=$(hdfs dfsadmin -report 2>/dev/null | grep "Dead datanodes" | awk '{print $4}' || echo "0")
            if [ "$dead_nodes" != "0" ]; then
                warn "Warning: ${dead_nodes} dead datanodes detected"
            fi
            
            perf "HDFS_CHECK status=healthy usage=${hdfs_capacity}"
            return 0
        fi
        
        retry_count=$((retry_count + 1))
        warn "HDFS connection failed. Retry $retry_count/$max_retries..."
        sleep 2
    done
    
    error "Cannot connect to HDFS after $max_retries attempts"
    exit 1
}

function validate_input {
    animated_header "INPUT DATA VALIDATION"
    
    info "Validating: ${CYAN}$INPUT_PATH${NC}"
    
    if ! hdfs dfs -test -e "$INPUT_PATH"; then
        error "Input file does not exist in HDFS: $INPUT_PATH"
        exit 1
    fi
    
    # Get file statistics
    local file_size=$(hdfs dfs -du -s "$INPUT_PATH" | awk '{print $1}')
    local file_size_mb=$((file_size / 1024 / 1024))
    local record_count=$(hdfs dfs -cat "$INPUT_PATH" 2>/dev/null | wc -l || echo "0")
    
    info "File size: ${GREEN}${file_size_mb} MB${NC}"
    info "Record count: ${GREEN}${record_count}${NC}"
    
    if [ "$file_size_mb" -eq 0 ]; then
        error "Input file is empty"
        exit 1
    fi
    
    if [ "$record_count" -lt "$NUM_CLUSTERS" ]; then
        error "Record count ($record_count) is less than number of clusters ($NUM_CLUSTERS)"
        exit 1
    fi
    
    success "Input validation passed ✓"
    
    # Store metrics
    echo "{\"file_size_mb\": $file_size_mb, \"record_count\": $record_count}" > "$METRICS_DIR/input_stats_${RUN_ID}.json"
    perf "INPUT_VALIDATION size_mb=${file_size_mb} records=${record_count}"
}

# ===========================
# BUILD & DEPLOYMENT
# ===========================
function build_project {
    animated_header "MAVEN BUILD PROCESS"
    
    cd "$PROJECT_DIR" || { error "Project directory not found: $PROJECT_DIR"; exit 1; }
    
    local build_start=$(date +%s)
    
    local steps=("clean" "compile" "test" "package")
    local step_num=0
    
    for step in "${steps[@]}"; do
        step_num=$((step_num + 1))
        echo -e "\n${YELLOW}[STEP $step_num/${#steps[@]}]${NC} ${BOLD}Maven $step${NC}"
        
        local temp_log=$(mktemp)
        local step_start=$(date +%s)
        
        case $step in
            clean)
                mvn clean > "$temp_log" 2>&1 &
                ;;
            compile)
                mvn compile > "$temp_log" 2>&1 &
                ;;
            test)
                mvn test -DskipTests > "$temp_log" 2>&1 &
                ;;
            package)
                mvn package -DskipTests > "$temp_log" 2>&1 &
                ;;
        esac
        
        local pid=$!
        show_spinner $pid "  Processing"
        wait $pid
        local exit_code=$?
        
        local step_end=$(date +%s)
        local step_duration=$((step_end - step_start))
        
        cat "$temp_log" >> "$LOG_FILE"
        rm -f "$temp_log"
        
        if [ $exit_code -ne 0 ]; then
            error "Maven $step failed"
            exit 1
        fi
        
        perf "BUILD_STEP step=$step duration=${step_duration}s"
    done
    
    if [ ! -f "target/$JAR_NAME" ]; then
        error "Build artifact not found: target/$JAR_NAME"
        exit 1
    fi
    
    local build_end=$(date +%s)
    local build_duration=$((build_end - build_start))
    local jar_size=$(du -h "target/$JAR_NAME" | cut -f1)
    
    echo -e "\n${GREEN}╔════════════════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}║              BUILD SUCCESSFUL                      ║${NC}"
    echo -e "${GREEN}╠════════════════════════════════════════════════════╣${NC}"
    printf "${GREEN}║${NC} %-25s : ${CYAN}%-21s${GREEN}║${NC}\n" "JAR File" "$JAR_NAME"
    printf "${GREEN}║${NC} %-25s : ${CYAN}%-21s${GREEN}║${NC}\n" "Size" "$jar_size"
    printf "${GREEN}║${NC} %-25s : ${CYAN}%-21s${GREEN}║${NC}\n" "Build Time" "${build_duration}s"
    printf "${GREEN}║${NC} %-25s : ${CYAN}%-21s${GREEN}║${NC}\n" "Location" "target/"
    echo -e "${GREEN}╚════════════════════════════════════════════════════╝${NC}\n"
    
    success "Build completed in ${build_duration}s ✓"
    perf "BUILD_COMPLETE duration=${build_duration}s jar_size=${jar_size}"
}

# ===========================
# SPARK EXECUTION
# ===========================
function run_spark_job {
    animated_header "SPARK JOB EXECUTION"
    
    echo -e "${CYAN}Configuration:${NC}"
    echo -e "  ${DIM}├─${NC} Clusters         : ${GREEN}$NUM_CLUSTERS${NC}"
    echo -e "  ${DIM}├─${NC} Iterations       : ${GREEN}$NUM_ITERATIONS${NC}"
    echo -e "  ${DIM}├─${NC} Spark Master     : ${GREEN}$SPARK_MASTER${NC}"
    echo -e "  ${DIM}├─${NC} Executor Memory  : ${GREEN}$EXECUTOR_MEMORY${NC}"
    echo -e "  ${DIM}├─${NC} Driver Memory    : ${GREEN}$DRIVER_MEMORY${NC}"
    echo -e "  ${DIM}├─${NC} Executor Cores   : ${GREEN}$EXECUTOR_CORES${NC}"
    echo -e "  ${DIM}└─${NC} Num Executors    : ${GREEN}$NUM_EXECUTORS${NC}"
    echo
    
    local start_time=$(date +%s)
    
    info "Submitting Spark job..."
    
    spark-submit \
        --class parallel.kmeans.ParallelKMeans \
        --master "$SPARK_MASTER" \
        --executor-memory "$EXECUTOR_MEMORY" \
        --driver-memory "$DRIVER_MEMORY" \
        --executor-cores "$EXECUTOR_CORES" \
        --num-executors "$NUM_EXECUTORS" \
        --conf spark.ui.showConsoleProgress=true \
        --conf spark.sql.shuffle.partitions=200 \
        --conf spark.dynamicAllocation.enabled="$ENABLE_DYNAMIC_ALLOCATION" \
        --conf spark.eventLog.enabled=true \
        --conf spark.eventLog.dir="$LOG_DIR/spark-events" \
        --conf spark.executor.extraJavaOptions="-XX:+UseG1GC" \
        --conf spark.driver.extraJavaOptions="-XX:+UseG1GC" \
        "target/$JAR_NAME" \
        "$INPUT_PATH" \
        "$NUM_CLUSTERS" \
        "$NUM_ITERATIONS" 2>&1 | tee -a "$LOG_FILE" | tee "$RESULT_FILE"
    
    local exit_code=${PIPESTATUS[0]}
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    
    if [ $exit_code -eq 0 ]; then
        echo -e "\n${GREEN}╔════════════════════════════════════════════════════╗${NC}"
        echo -e "${GREEN}║          SPARK JOB COMPLETED SUCCESSFULLY          ║${NC}"
        echo -e "${GREEN}╠════════════════════════════════════════════════════╣${NC}"
        printf "${GREEN}║${NC} Execution Time    : ${CYAN}%-30s${GREEN}║${NC}\n" "${duration}s"
        printf "${GREEN}║${NC} Exit Code         : ${CYAN}%-30s${GREEN}║${NC}\n" "$exit_code"
        echo -e "${GREEN}╚════════════════════════════════════════════════════╝${NC}\n"
        
        perf "SPARK_JOB status=success duration=${duration}s exit_code=$exit_code"
        return 0
    else
        error "Spark job failed with exit code $exit_code"
        perf "SPARK_JOB status=failed duration=${duration}s exit_code=$exit_code"
        return 1
    fi
}

# ===========================
# ADVANCED ANALYTICS
# ===========================
function calculate_comprehensive_metrics {
    animated_header "COMPREHENSIVE METRICS CALCULATION"
    
    # Create enhanced Python script with full analytics
    cat > /tmp/calc_accuracy_${RUN_ID}.py << 'PYTHON_SCRIPT'
#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import math
import json
from collections import defaultdict

def euclidean_distance(p1, p2):
    return math.sqrt(sum((a - b) ** 2 for a, b in zip(p1, p2)))

def load_data(filepath):
    import subprocess
    data, labels = [], []
    
    try:
        if filepath.startswith('hdfs://'):
            cmd = ['hdfs', 'dfs', '-cat', filepath]
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, _ = proc.communicate()
            lines = stdout.strip().split('\n')
        else:
            with open(filepath, 'r') as f:
                lines = f.readlines()
        
        for line in lines:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            
            parts = line.split(',')
            if len(parts) >= 4:
                features = [float(x) for x in parts[:4]]
                data.append(features)
                if len(parts) > 4:
                    labels.append(parts[-1].strip())
        
        return data, labels
    except Exception as e:
        print >> sys.stderr, "Error loading data: %s" % str(e)
        return [], []

def load_centroids(result_file):
    centroids = []
    in_centers = False
    
    try:
        with open(result_file, 'r') as f:
            for line in f:
                if 'Cluster centers:' in line or 'Final cluster centers:' in line:
                    in_centers = True
                    continue
                
                if in_centers:
                    line = line.strip()
                    if not line or line.startswith('===') or line.startswith('---'):
                        break
                    
                    if '[' in line and ']' in line:
                        coords_str = line[line.index('['):line.index(']')+1]
                        coords_str = coords_str.replace('[', '').replace(']', '')
                        coords = [float(x.strip()) for x in coords_str.split(',')]
                        if len(coords) >= 4:
                            centroids.append(coords[:4])
        
        return centroids
    except Exception as e:
        print >> sys.stderr, "Error loading centroids: %s" % str(e)
        return []

def assign_to_clusters(data, centroids):
    assignments = []
    for point in data:
        distances = [euclidean_distance(point, c) for c in centroids]
        cluster_id = distances.index(min(distances))
        assignments.append(cluster_id)
    return assignments

def calculate_wcss(data, centroids, assignments):
    wcss = 0.0
    for i, point in enumerate(data):
        cluster_id = assignments[i]
        centroid = centroids[cluster_id]
        wcss += euclidean_distance(point, centroid) ** 2
    return wcss

def calculate_silhouette(data, assignments):
    unique_assignments = list(set(assignments))
    if len(unique_assignments) <= 1 or len(data) > 5000:
        return 0.0
    
    scores = []
    for i in range(len(data)):
        point = data[i]
        cluster_id = assignments[i]
        
        same_cluster = [j for j, c in enumerate(assignments) if c == cluster_id and j != i]
        if not same_cluster:
            scores.append(0.0)
            continue
        
        a_i = sum(euclidean_distance(point, data[j]) for j in same_cluster) / float(len(same_cluster))
        
        other_clusters = list(set(assignments) - set([cluster_id]))
        if not other_clusters:
            scores.append(0.0)
            continue
        
        b_i = float('inf')
        for other_cluster in other_clusters:
            other_points = [j for j, c in enumerate(assignments) if c == other_cluster]
            if other_points:
                mean_dist = sum(euclidean_distance(point, data[j]) for j in other_points) / float(len(other_points))
                b_i = min(b_i, mean_dist)
        
        scores.append((b_i - a_i) / max(a_i, b_i) if max(a_i, b_i) > 0 else 0.0)
    
    return sum(scores) / float(len(scores)) if scores else 0.0

def calculate_davies_bouldin(data, centroids, assignments):
    n_clusters = len(centroids)
    if n_clusters <= 1:
        return 0.0
    
    cluster_scatter = []
    for k in range(n_clusters):
        cluster_points = [data[i] for i in range(len(data)) if assignments[i] == k]
        if not cluster_points:
            cluster_scatter.append(0.0)
            continue
        
        scatter = sum(euclidean_distance(p, centroids[k]) for p in cluster_points)
        cluster_scatter.append(scatter / float(len(cluster_points)))
    
    db_index = 0.0
    for i in range(n_clusters):
        max_ratio = 0.0
        for j in range(n_clusters):
            if i != j:
                centroid_dist = euclidean_distance(centroids[i], centroids[j])
                if centroid_dist > 0:
                    ratio = (cluster_scatter[i] + cluster_scatter[j]) / centroid_dist
                    max_ratio = max(max_ratio, ratio)
        db_index += max_ratio
    
    return db_index / float(n_clusters)

def calculate_accuracy(assignments, true_labels):
    if not true_labels:
        return 0.0
    
    clusters = defaultdict(list)
    for i, cluster_id in enumerate(assignments):
        clusters[cluster_id].append(true_labels[i])
    
    cluster_to_label = {}
    for cluster_id in clusters:
        labels_list = clusters[cluster_id]
        counts = {}
        for label in labels_list:
            counts[label] = counts.get(label, 0) + 1
        most_common = max(counts.items(), key=lambda x: x[1])[0]
        cluster_to_label[cluster_id] = most_common
    
    correct = sum(1 for i, cluster_id in enumerate(assignments) 
                  if cluster_to_label.get(cluster_id) == true_labels[i])
    
    return correct / float(len(assignments)) if assignments else 0.0

def calculate_cluster_stats(data, assignments, centroids):
    stats = []
    for k in range(len(centroids)):
        cluster_points = [data[i] for i in range(len(data)) if assignments[i] == k]
        
        if not cluster_points:
            stats.append({
                'cluster_id': k,
                'size': 0,
                'density': 0.0,
                'radius': 0.0,
                'diameter': 0.0
            })
            continue
        
        distances = [euclidean_distance(p, centroids[k]) for p in cluster_points]
        avg_distance = sum(distances) / len(distances)
        max_distance = max(distances)
        
        # Diameter: max distance between any two points
        diameter = 0.0
        if len(cluster_points) > 1:
            for i in range(min(100, len(cluster_points))):
                for j in range(i+1, min(100, len(cluster_points))):
                    d = euclidean_distance(cluster_points[i], cluster_points[j])
                    diameter = max(diameter, d)
        
        stats.append({
            'cluster_id': k,
            'size': len(cluster_points),
            'density': 1.0 / (avg_distance + 0.0001),
            'radius': avg_distance,
            'diameter': diameter
        })
    
    return stats

def main():
    if len(sys.argv) < 4:
        print "Usage: script.py <input_data> <result_file> <output_json>"
        sys.exit(1)
    
    input_file = sys.argv[1]
    result_file = sys.argv[2]
    output_json = sys.argv[3]
    
    print "Loading data from %s..." % input_file
    data, true_labels = load_data(input_file)
    
    if not data:
        print "Error: No data loaded"
        sys.exit(1)
    
    print "Loaded %d samples" % len(data)
    
    print "Loading centroids from %s..." % result_file
    centroids = load_centroids(result_file)
    
    if not centroids:
        print "Error: No centroids found"
        sys.exit(1)
    
    print "Loaded %d centroids" % len(centroids)
    
    print "Assigning samples to clusters..."
    assignments = assign_to_clusters(data, centroids)
    
    print "Calculating metrics..."
    wcss = calculate_wcss(data, centroids, assignments)
    silhouette = calculate_silhouette(data, assignments)
    davies_bouldin = calculate_davies_bouldin(data, centroids, assignments)
    
    accuracy = 0.0
    if true_labels:
        accuracy = calculate_accuracy(assignments, true_labels)
    
    cluster_stats = calculate_cluster_stats(data, assignments, centroids)
    
    # Count distribution
    distribution = {}
    for a in assignments:
        distribution[a] = distribution.get(a, 0) + 1
    
    # Create results dictionary
    results = {
        'dataset_info': {
            'input_path': input_file,
            'num_samples': len(data),
            'num_features': len(data[0]) if data else 0,
            'num_clusters': len(centroids),
            'has_labels': len(true_labels) > 0
        },
        'quality_metrics': {
            'wcss': round(wcss, 4),
            'silhouette_score': round(silhouette, 4),
            'davies_bouldin_index': round(davies_bouldin, 4),
            'accuracy': round(accuracy, 4) if true_labels else None
        },
        'cluster_stats': cluster_stats,
        'distribution': distribution
    }
    
    # Write JSON output
    with open(output_json, 'w') as f:
        json.dump(results, f, indent=2)
    
    print "\n" + "="*60
    print "METRICS SUMMARY"
    print "="*60
    print "WCSS: %.4f" % wcss
    print "Silhouette Score: %.4f" % silhouette
    print "Davies-Bouldin Index: %.4f" % davies_bouldin
    if true_labels:
        print "Accuracy: %.2f%%" % (accuracy * 100)
    print "="*60
    print "\nResults saved to: %s" % output_json

if __name__ == "__main__":
    main()
PYTHON_SCRIPT
    
    chmod +x /tmp/calc_accuracy_${RUN_ID}.py
    
    local metrics_json="$METRICS_DIR/comprehensive_${RUN_ID}.json"
    
    python /tmp/calc_accuracy_${RUN_ID}.py "$INPUT_PATH" "$RESULT_FILE" "$metrics_json" 2>&1 | while IFS= read -r line; do
        echo -e "${DIM}  $line${NC}"
    done | tee -a "$LOG_FILE"
    
    if [ -f "$metrics_json" ]; then
        success "Comprehensive metrics calculated ✓"
        
        # Display key metrics
        if command -v jq &> /dev/null; then
            echo -e "\n${CYAN}Quality Metrics:${NC}"
            jq -r '.quality_metrics | to_entries[] | "  \(.key): \(.value)"' "$metrics_json" | while read line; do
                echo -e "${WHITE}$line${NC}"
            done
        fi
        
        perf "METRICS_CALCULATION status=success"
    else
        warn "Metrics calculation incomplete"
        perf "METRICS_CALCULATION status=warning"
    fi
}

# ===========================
# PROFESSIONAL HTML REPORT GENERATION
# ===========================
function generate_professional_report {
    animated_header "GENERATING PROFESSIONAL REPORT"
    
    local metrics_json="$METRICS_DIR/comprehensive_${RUN_ID}.json"
    local execution_time=$(grep "Execution Time" "$LOG_FILE" | tail -1 | awk '{print $NF}' || echo "N/A")
    
    info "Creating HTML report: $FINAL_REPORT"
    
    cat > "$FINAL_REPORT" << 'EOF'
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>K-Means Clustering Analysis Report</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 20px;
            color: #333;
        }
        
        .container {
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            border-radius: 20px;
            box-shadow: 0 20px 60px rgba(0,0,0,0.3);
            overflow: hidden;
        }
        
        .header {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 40px;
            text-align: center;
        }
        
        .header h1 {
            font-size: 2.5em;
            margin-bottom: 10px;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
        }
        
        .header .subtitle {
            font-size: 1.2em;
            opacity: 0.9;
        }
        
        .meta-info {
            background: #f8f9fa;
            padding: 20px 40px;
            border-bottom: 3px solid #667eea;
        }
        
        .meta-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
        }
        
        .meta-item {
            background: white;
            padding: 15px;
            border-radius: 10px;
            border-left: 4px solid #667eea;
            box-shadow: 0 2px 5px rgba(0,0,0,0.1);
        }
        
        .meta-item label {
            font-size: 0.85em;
            color: #666;
            text-transform: uppercase;
            letter-spacing: 1px;
            display: block;
            margin-bottom: 5px;
        }
        
        .meta-item value {
            font-size: 1.1em;
            font-weight: bold;
            color: #333;
        }
        
        .content {
            padding: 40px;
        }
        
        .section {
            margin-bottom: 40px;
        }
        
        .section-title {
            font-size: 1.8em;
            color: #667eea;
            margin-bottom: 20px;
            padding-bottom: 10px;
            border-bottom: 3px solid #667eea;
            display: flex;
            align-items: center;
            gap: 10px;
        }
        
        .icon {
            width: 30px;
            height: 30px;
        }
        
        .metric-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-top: 20px;
        }
        
        .metric-card {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 25px;
            border-radius: 15px;
            box-shadow: 0 5px 15px rgba(102,126,234,0.3);
            transition: transform 0.3s ease;
        }
        
        .metric-card:hover {
            transform: translateY(-5px);
        }
        
        .metric-label {
            font-size: 0.9em;
            opacity: 0.9;
            margin-bottom: 10px;
        }
        
        .metric-value {
            font-size: 2.5em;
            font-weight: bold;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.2);
        }
        
        .metric-unit {
            font-size: 0.5em;
            opacity: 0.8;
        }
        
        table {
            width: 100%;
            border-collapse: collapse;
            margin-top: 20px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            border-radius: 10px;
            overflow: hidden;
        }
        
        thead {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
        }
        
        th {
            padding: 15px;
            text-align: left;
            font-weight: 600;
            text-transform: uppercase;
            font-size: 0.9em;
            letter-spacing: 1px;
        }
        
        td {
            padding: 15px;
            border-bottom: 1px solid #e9ecef;
        }
        
        tbody tr:hover {
            background: #f8f9fa;
        }
        
        .progress-bar {
            background: #e9ecef;
            height: 25px;
            border-radius: 12px;
            overflow: hidden;
            position: relative;
        }
        
        .progress-fill {
            height: 100%;
            background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
            display: flex;
            align-items: center;
            justify-content: center;
            color: white;
            font-weight: bold;
            font-size: 0.85em;
            transition: width 0.3s ease;
        }
        
        .status-badge {
            display: inline-block;
            padding: 5px 15px;
            border-radius: 20px;
            font-size: 0.85em;
            font-weight: bold;
        }
        
        .status-success {
            background: #28a745;
            color: white;
        }
        
        .status-warning {
            background: #ffc107;
            color: #333;
        }
        
        .interpretation {
            background: #f8f9fa;
            border-left: 4px solid #667eea;
            padding: 20px;
            border-radius: 5px;
            margin-top: 20px;
        }
        
        .interpretation h4 {
            color: #667eea;
            margin-bottom: 10px;
        }
        
        .footer {
            background: #2c3e50;
            color: white;
            padding: 20px 40px;
            text-align: center;
        }
        
        .chart-container {
            margin: 20px 0;
            padding: 20px;
            background: #f8f9fa;
            border-radius: 10px;
        }
        
        @media print {
            body {
                background: white;
                padding: 0;
            }
            
            .container {
                box-shadow: none;
            }
            
            .metric-card {
                page-break-inside: avoid;
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📊 K-Means Clustering Analysis</h1>
            <div class="subtitle">Professional Machine Learning Report</div>
        </div>
        
        <div class="meta-info">
            <div class="meta-grid">
                <div class="meta-item">
                    <label>Report Generated</label>
                    <value id="timestamp"></value>
                </div>
                <div class="meta-item">
                    <label>Run ID</label>
                    <value id="run-id"></value>
                </div>
                <div class="meta-item">
                    <label>Dataset</label>
                    <value id="dataset"></value>
                </div>
                <div class="meta-item">
                    <label>Execution Status</label>
                    <value><span class="status-badge status-success">✓ SUCCESS</span></value>
                </div>
            </div>
        </div>
        
        <div class="content">
            <!-- Configuration Section -->
            <div class="section">
                <h2 class="section-title">
                    <svg class="icon" viewBox="0 0 24 24" fill="currentColor">
                        <path d="M12 8c1.1 0 2-.9 2-2s-.9-2-2-2-2 .9-2 2 .9 2 2 2zm0 2c-1.1 0-2 .9-2 2s.9 2 2 2 2-.9 2-2-.9-2-2-2zm0 6c-1.1 0-2 .9-2 2s.9 2 2 2 2-.9 2-2-.9-2-2-2z"/>
                    </svg>
                    Configuration Parameters
                </h2>
                <div class="metric-grid">
                    <div class="metric-card">
                        <div class="metric-label">Number of Clusters (K)</div>
                        <div class="metric-value" id="num-clusters"></div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-label">Max Iterations</div>
                        <div class="metric-value" id="num-iterations"></div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-label">Total Samples</div>
                        <div class="metric-value" id="num-samples"></div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-label">Execution Time</div>
                        <div class="metric-value" id="exec-time"></div>
                    </div>
                </div>
            </div>
            
            <!-- Quality Metrics Section -->
            <div class="section">
                <h2 class="section-title">
                    <svg class="icon" viewBox="0 0 24 24" fill="currentColor">
                        <path d="M19 3H5c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h14c1.1 0 2-.9 2-2V5c0-1.1-.9-2-2-2zM9 17H7v-7h2v7zm4 0h-2V7h2v10zm4 0h-2v-4h2v4z"/>
                    </svg>
                    Quality Metrics
                </h2>
                <div class="metric-grid">
                    <div class="metric-card">
                        <div class="metric-label">WCSS (Within-Cluster Sum of Squares)</div>
                        <div class="metric-value" id="wcss"></div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-label">Silhouette Score</div>
                        <div class="metric-value" id="silhouette"></div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-label">Davies-Bouldin Index</div>
                        <div class="metric-value" id="davies-bouldin"></div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-label">Clustering Accuracy</div>
                        <div class="metric-value"><span id="accuracy"></span><span class="metric-unit">%</span></div>
                    </div>
                </div>
                
                <div class="interpretation">
                    <h4>📖 Interpretation Guide</h4>
                    <p><strong>WCSS:</strong> Lower values indicate more compact clusters. Use the elbow method to find optimal K.</p>
                    <p><strong>Silhouette Score:</strong> Ranges from -1 to 1. Values > 0.5 indicate good clustering, > 0.7 is excellent.</p>
                    <p><strong>Davies-Bouldin Index:</strong> Lower values indicate better separation between clusters. Values < 1.0 are preferred.</p>
                    <p><strong>Accuracy:</strong> When true labels are available, shows how well clusters align with ground truth.</p>
                </div>
            </div>
            
            <!-- Cluster Distribution Section -->
            <div class="section">
                <h2 class="section-title">
                    <svg class="icon" viewBox="0 0 24 24" fill="currentColor">
                        <path d="M9 11H7v2h2v-2zm4 0h-2v2h2v-2zm4 0h-2v2h2v-2zm2-7h-1V2h-2v2H8V2H6v2H5c-1.11 0-1.99.9-1.99 2L3 20c0 1.1.89 2 2 2h14c1.1 0 2-.9 2-2V6c0-1.1-.9-2-2-2zm0 16H5V9h14v11z"/>
                    </svg>
                    Cluster Distribution
                </h2>
                <table id="cluster-table">
                    <thead>
                        <tr>
                            <th>Cluster ID</th>
                            <th>Sample Count</th>
                            <th>Percentage</th>
                            <th>Distribution</th>
                        </tr>
                    </thead>
                    <tbody id="cluster-tbody"></tbody>
                </table>
            </div>
            
            <!-- Cluster Statistics Section -->
            <div class="section">
                <h2 class="section-title">
                    <svg class="icon" viewBox="0 0 24 24" fill="currentColor">
                        <path d="M3 5v14c0 1.1.89 2 2 2h14c1.1 0 2-.9 2-2V5c0-1.1-.9-2-2-2H5c-1.11 0-2 .9-2 2zm12 4c0 1.66-1.34 3-3 3s-3-1.34-3-3 1.34-3 3-3 3 1.34 3 3zm-9 8c0-2 4-3.1 6-3.1s6 1.1 6 3.1v1H6v-1z"/>
                    </svg>
                    Detailed Cluster Statistics
                </h2>
                <table id="stats-table">
                    <thead>
                        <tr>
                            <th>Cluster</th>
                            <th>Size</th>
                            <th>Density</th>
                            <th>Radius</th>
                            <th>Diameter</th>
                        </tr>
                    </thead>
                    <tbody id="stats-tbody"></tbody>
                </table>
            </div>
            
            <!-- Performance Section -->
            <div class="section">
                <h2 class="section-title">
                    <svg class="icon" viewBox="0 0 24 24" fill="currentColor">
                        <path d="M11.99 2C6.47 2 2 6.48 2 12s4.47 10 9.99 10C17.52 22 22 17.52 22 12S17.52 2 11.99 2zM12 20c-4.42 0-8-3.58-8-8s3.58-8 8-8 8 3.58 8 8-3.58 8-8 8zm.5-13H11v6l5.25 3.15.75-1.23-4.5-2.67z"/>
                    </svg>
                    Performance Summary
                </h2>
                <div class="metric-grid">
                    <div class="metric-card">
                        <div class="metric-label">Spark Master</div>
                        <div class="metric-value" style="font-size: 1.2em;" id="spark-master"></div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-label">Executor Memory</div>
                        <div class="metric-value" style="font-size: 1.5em;" id="executor-memory"></div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-label">Driver Memory</div>
                        <div class="metric-value" style="font-size: 1.5em;" id="driver-memory"></div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-label">Build Time</div>
                        <div class="metric-value" style="font-size: 1.5em;" id="build-time"></div>
                    </div>
                </div>
            </div>
        </div>
        
        <div class="footer">
            <p>Generated by K-Means Clustering Pipeline v2.0</p>
            <p>Apache Spark • Professional Machine Learning Analytics</p>
        </div>
    </div>
    
    <script>
        // Data will be injected here
        const REPORT_DATA = INJECT_DATA_HERE;
        
        // Populate report
        document.getElementById('timestamp').textContent = REPORT_DATA.timestamp;
        document.getElementById('run-id').textContent = REPORT_DATA.run_id;
        document.getElementById('dataset').textContent = REPORT_DATA.dataset;
        document.getElementById('num-clusters').textContent = REPORT_DATA.config.num_clusters;
        document.getElementById('num-iterations').textContent = REPORT_DATA.config.num_iterations;
        document.getElementById('num-samples').textContent = REPORT_DATA.config.num_samples;
        document.getElementById('exec-time').textContent = REPORT_DATA.performance.execution_time;
        document.getElementById('spark-master').textContent = REPORT_DATA.config.spark_master;
        document.getElementById('executor-memory').textContent = REPORT_DATA.config.executor_memory;
        document.getElementById('driver-memory').textContent = REPORT_DATA.config.driver_memory;
        document.getElementById('build-time').textContent = REPORT_DATA.performance.build_time;
        
        // Quality metrics
        document.getElementById('wcss').textContent = REPORT_DATA.metrics.wcss;
        document.getElementById('silhouette').textContent = REPORT_DATA.metrics.silhouette_score;
        document.getElementById('davies-bouldin').textContent = REPORT_DATA.metrics.davies_bouldin_index;
        document.getElementById('accuracy').textContent = REPORT_DATA.metrics.accuracy ? (REPORT_DATA.metrics.accuracy * 100).toFixed(2) : 'N/A';
        
        // Cluster distribution
        const tbody = document.getElementById('cluster-tbody');
        Object.entries(REPORT_DATA.distribution).forEach(([cluster, count]) => {
            const percentage = (count / REPORT_DATA.config.num_samples * 100).toFixed(1);
            const row = tbody.insertRow();
            row.innerHTML = `
                <td><strong>Cluster ${cluster}</strong></td>
                <td>${count}</td>
                <td>${percentage}%</td>
                <td>
                    <div class="progress-bar">
                        <div class="progress-fill" style="width: ${percentage}%">${percentage}%</div>
                    </div>
                </td>
            `;
        });
        
        // Cluster statistics
        const statsTbody = document.getElementById('stats-tbody');
        REPORT_DATA.cluster_stats.forEach(stat => {
            const row = statsTbody.insertRow();
            row.innerHTML = `
                <td><strong>Cluster ${stat.cluster_id}</strong></td>
                <td>${stat.size}</td>
                <td>${stat.density.toFixed(4)}</td>
                <td>${stat.radius.toFixed(4)}</td>
                <td>${stat.diameter.toFixed(4)}</td>
            `;
        });
    </script>
</body>
</html>
EOF
    
    # Extract build time from perf log
    local build_time=$(grep "BUILD_COMPLETE" "$PERF_LOG" | awk -F'duration=' '{print $2}' | awk '{print $1}' || echo "N/A")
    
    # Create JSON data for report
    local report_data_json=$(cat "$metrics_json" 2>/dev/null || echo '{"quality_metrics": {}, "cluster_stats": [], "distribution": {}}')
    
    # Build comprehensive data object
    local comprehensive_data=$(cat << JSONEOF
{
    "timestamp": "$TIMESTAMP",
    "run_id": "$RUN_ID",
    "dataset": "$(basename $INPUT_PATH)",
    "config": {
        "num_clusters": $NUM_CLUSTERS,
        "num_iterations": $NUM_ITERATIONS,
        "num_samples": $(jq -r '.dataset_info.num_samples // 0' "$metrics_json" 2>/dev/null || echo 0),
        "spark_master": "$SPARK_MASTER",
        "executor_memory": "$EXECUTOR_MEMORY",
        "driver_memory": "$DRIVER_MEMORY"
    },
    "metrics": $(jq '.quality_metrics // {}' "$metrics_json" 2>/dev/null || echo '{}'),
    "cluster_stats": $(jq '.cluster_stats // []' "$metrics_json" 2>/dev/null || echo '[]'),
    "distribution": $(jq '.distribution // {}' "$metrics_json" 2>/dev/null || echo '{}'),
    "performance": {
        "execution_time": "$execution_time",
        "build_time": "$build_time"
    }
}
JSONEOF
)
    
    # Inject data into HTML
    sed -i "s/const REPORT_DATA = INJECT_DATA_HERE;/const REPORT_DATA = $comprehensive_data;/" "$FINAL_REPORT"
    
    # Create symlink to latest report
    ln -sf "$(basename $FINAL_REPORT)" "$REPORT_DIR/latest_report.html"
    
    success "Professional HTML report generated ✓"
    info "📄 Report location: ${CYAN}$FINAL_REPORT${NC}"
    info "🔗 Latest report: ${CYAN}$REPORT_DIR/latest_report.html${NC}"
    
    perf "REPORT_GENERATION status=success file=$FINAL_REPORT"
}

# ===========================
# MAIN EXECUTION
# ===========================
function print_final_summary {
    animated_header "PIPELINE EXECUTION SUMMARY"
    
    local total_duration=$(grep "SPARK_JOB" "$PERF_LOG" | awk -F'duration=' '{print $2}' | awk '{print $1}' | sed 's/s//' || echo "0")
    
    echo -e "${GREEN}╔═══════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}║                   🎉 PIPELINE COMPLETED SUCCESSFULLY 🎉       ║${NC}"
    echo -e "${GREEN}╠═══════════════════════════════════════════════════════════════╣${NC}"
    printf "${GREEN}║${NC} ${BOLD}%-30s${NC} : ${CYAN}%-28s${GREEN}║${NC}\n" "Run ID" "$RUN_ID"
    printf "${GREEN}║${NC} ${BOLD}%-30s${NC} : ${CYAN}%-28s${GREEN}║${NC}\n" "Timestamp" "$TIMESTAMP"
    printf "${GREEN}║${NC} ${BOLD}%-30s${NC} : ${CYAN}%-28s${GREEN}║${NC}\n" "Dataset" "$(basename $INPUT_PATH)"
    printf "${GREEN}║${NC} ${BOLD}%-30s${NC} : ${CYAN}%-28s${GREEN}║${NC}\n" "Total Duration" "${total_duration}s"
    echo -e "${GREEN}╠═══════════════════════════════════════════════════════════════╣${NC}"
    printf "${GREEN}║${NC} ${BOLD}%-30s${NC} : ${YELLOW}%-28s${GREEN}║${NC}\n" "HTML Report" "$(basename $FINAL_REPORT)"
    printf "${GREEN}║${NC} ${BOLD}%-30s${NC} : ${YELLOW}%-28s${GREEN}║${NC}\n" "Results File" "$(basename $RESULT_FILE)"
    printf "${GREEN}║${NC} ${BOLD}%-30s${NC} : ${YELLOW}%-28s${GREEN}║${NC}\n" "Metrics JSON" "$(basename $metrics_json)"
    printf "${GREEN}║${NC} ${BOLD}%-30s${NC} : ${YELLOW}%-28s${GREEN}║${NC}\n" "Log File" "$(basename $LOG_FILE)"
    echo -e "${GREEN}╚═══════════════════════════════════════════════════════════════╝${NC}\n"
    
    echo -e "${MAGENTA}${BOLD}"
    cat << "EOF"
    ╔═══════════════════════════════════════════════════════╗
    ║                                                       ║
    ║              ✨ SUCCESS ✨                            ║
    ║         All Pipeline Tasks Completed                  ║
    ║                                                       ║
    ╚═══════════════════════════════════════════════════════╝
EOF
    echo -e "${NC}"
    
    echo -e "${CYAN}📊 View Report:${NC}"
    echo -e "   ${WHITE}file://$FINAL_REPORT${NC}"
    echo -e "\n${CYAN}💡 Quick Access:${NC}"
    echo -e "   ${DIM}open $FINAL_REPORT${NC}"
    echo
}

function show_usage {
    cat << EOF
${BOLD}K-Means Clustering Pipeline - Professional Edition${NC}

${BOLD}Usage:${NC} $0 [OPTIONS]

${BOLD}Required Options:${NC}
    -i, --input PATH          Input HDFS path

${BOLD}Clustering Options:${NC}
    -k, --clusters NUM        Number of clusters (default: $NUM_CLUSTERS)
    -n, --iterations NUM      Max iterations (default: $NUM_ITERATIONS)
    --auto-k                  Enable automatic K optimization
    --k-range MIN,MAX         K range for optimization (default: $K_RANGE)

${BOLD}Spark Configuration:${NC}
    -m, --master URL          Spark master URL (default: $SPARK_MASTER)
    --executor-mem SIZE       Executor memory (default: $EXECUTOR_MEMORY)
    --driver-mem SIZE         Driver memory (default: $DRIVER_MEMORY)
    --executor-cores NUM      Executor cores (default: $EXECUTOR_CORES)
    --num-executors NUM       Number of executors (default: $NUM_EXECUTORS)

${BOLD}Pipeline Options:${NC}
    --no-viz                  Disable visualizations
    --no-profile              Disable performance profiling
    --retries NUM             Max retry attempts (default: $MAX_RETRIES)
    -h, --help                Show this help message

${BOLD}Examples:${NC}
    # Basic run
    $0 -i hdfs:///data/iris_dataset -k 3

    # Advanced run with custom Spark config
    $0 -i hdfs:///data/large_dataset -k 5 -n 50 \\
       --executor-mem 4g --driver-mem 2g --num-executors 4
    
    # Auto-optimize K value
    $0 -i hdfs:///data/dataset --auto-k --k-range 2,10

EOF
}

function parse_args {
    if [ $# -eq 0 ]; then
        show_usage
        exit 0
    fi
    
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
            --executor-cores)
                EXECUTOR_CORES="$2"
                shift 2
                ;;
            --num-executors)
                NUM_EXECUTORS="$2"
                shift 2
                ;;
            --auto-k)
                AUTO_OPTIMIZE_K=true
                shift
                ;;
            --k-range)
                K_RANGE="$2"
                shift 2
                ;;
            --no-viz)
                ENABLE_VISUALIZATION=false
                shift
                ;;
            --no-profile)
                ENABLE_PROFILING=false
                shift
                ;;
            --retries)
                MAX_RETRIES="$2"
                shift 2
                ;;
            *)
                error "Unknown option: $1"
                show_usage
                exit 1
                ;;
        esac
    done
    
    # Validate required arguments
    if [ -z "$INPUT_PATH" ]; then
        error "Input path is required. Use -i or --input"
        show_usage
        exit 1
    fi
}

function main {
    print_banner
    parse_args "$@"
    
    info "🚀 Initializing K-Means Clustering Pipeline..."
    info "Run ID: ${CYAN}$RUN_ID${NC}"
    echo
    
    sleep 0.5
    
    # Pipeline stages
    check_system_resources
    check_prerequisites
    check_hdfs_health
    validate_input
    build_project
    
    if run_spark_job; then
        calculate_comprehensive_metrics
        generate_professional_report
        print_final_summary
        
        # Archive successful run
        info "Archiving results..."
        tar -czf "$BACKUP_DIR/run_${RUN_ID}.tar.gz" \
            "$LOG_FILE" "$RESULT_FILE" "$METRICS_DIR/"*"${RUN_ID}"* "$FINAL_REPORT" 2>/dev/null || true
        
        success "Pipeline completed successfully! ✓"
        exit 0
    else
        error "Pipeline failed. Check logs for details: $LOG_FILE"
        exit 1
    fi
}

# Execute main function
main "$@"
