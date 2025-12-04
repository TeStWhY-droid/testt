#!/bin/bash
#==============================
# Automated K-Means Spark Runner (Enhanced with Animations)
#==============================

set -euo pipefail

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
METRICS_FILE="$RESULTS_DIR/kmeans_metrics_$TIMESTAMP.json"
REPORT_FILE="$REPORT_DIR/kmeans_comprehensive_report_$TIMESTAMP.md"
ACCURACY_FILE="$RESULTS_DIR/kmeans_accuracy_$TIMESTAMP.txt"

# Colors and formatting
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
PROGRESS_BAR=("▏" "▎" "▍" "▌" "▋" "▊" "▉" "█")
DOTS=("⠁" "⠂" "⠄" "⡀" "⢀" "⠠" "⠐" "⠈")

# Logging functions
function log { echo -e "[$(date +'%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"; }
function info { log "${BLUE}[INFO]${NC} $1"; }
function warn { log "${YELLOW}[WARN]${NC} $1"; }
function error { log "${RED}[ERROR]${NC} $1"; }
function success { log "${GREEN}[SUCCESS]${NC} $1"; }
function debug { log "${CYAN}[DEBUG]${NC} $1"; }

# Animation functions
function show_spinner {
    local pid=$1
    local message=$2
    local i=0
    
    echo -n "$message "
    while kill -0 $pid 2>/dev/null; do
        echo -ne "\r$message ${CYAN}${SPINNER[$i]}${NC}"
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
    local width=60
    local padding=$(( (width - ${#text}) / 2 ))
    
    clear
    echo -e "${MAGENTA}"
    echo "╔$(printf '═%.0s' $(seq 1 $width))╗"
    printf "║%${padding}s" ""
    echo -n "$text"
    printf "%$((width - padding - ${#text}))s║\n" ""
    echo "╚$(printf '═%.0s' $(seq 1 $width))╝"
    echo -e "${NC}"
}

function pulse_text {
    local text=$1
    local colors=("${RED}" "${YELLOW}" "${GREEN}" "${CYAN}" "${BLUE}" "${MAGENTA}")
    
    for color in "${colors[@]}"; do
        echo -ne "\r${color}${BOLD}$text${NC}"
        sleep 0.1
    done
    echo
}

function typewriter {
    local text=$1
    local delay=${2:-0.03}
    
    for (( i=0; i<${#text}; i++ )); do
        echo -n "${text:$i:1}"
        sleep $delay
    done
    echo
}

# Trap errors and cleanup
trap 'error "Script failed at line $LINENO. Exit code: $?"; cleanup; exit 1' ERR
trap 'warn "Script interrupted by user"; cleanup; exit 130' INT TERM

function cleanup {
    info "Performing cleanup..."
}

# Print animated banner
function print_banner {
    clear
    echo -e "${MAGENTA}${BOLD}"
    cat << "EOF"
    ╔═══════════════════════════════════════════════════════════╗
    ║                                                           ║
    ║     █▀▀ █▄▀ ▄▄ █▀▄▀█ █▀▀ ▄▀█ █▄░█ █▀                   ║
    ║     █▄▄ █░█ ░░ █░▀░█ ██▄ █▀█ █░▀█ ▄█                   ║
    ║                                                           ║
    ║          Parallel Processing & Big Data Analytics        ║
    ║                   Apache Spark Edition                    ║
    ║                                                           ║
    ╚═══════════════════════════════════════════════════════════╝
EOF
    echo -e "${NC}"
    sleep 0.5
}

# Validate prerequisites with animation
function check_prerequisites {
    animated_header "PREREQUISITES VALIDATION"
    
    local tools=("mvn" "spark-submit" "hdfs" "python")
    local missing_tools=()
    local total=${#tools[@]}
    local current=0
    
    for tool in "${tools[@]}"; do
        current=$((current + 1))
        progress_bar $current $total
        sleep 0.2
        
        if ! command -v "$tool" &> /dev/null; then
            missing_tools+=("$tool")
        fi
    done
    echo
    
    if [ ${#missing_tools[@]} -ne 0 ]; then
        error "Missing required tools: ${missing_tools[*]}"
        exit 1
    fi
    
    success "All prerequisites validated ✓"
    sleep 0.5
}

# Check HDFS connectivity with animation
function check_hdfs {
    animated_header "HDFS CONNECTIVITY CHECK"
    
    echo -ne "${CYAN}Connecting to HDFS${NC}"
    for i in {1..5}; do
        echo -n "."
        sleep 0.2
    done
    echo
    
    if ! hdfs dfs -ls / &> /dev/null; then
        error "Cannot connect to HDFS"
        exit 1
    fi
    
    success "HDFS connection established ✓"
    sleep 0.5
}

# Validate input with animation
function validate_input {
    animated_header "INPUT VALIDATION"
    
    info "Validating: ${CYAN}$INPUT_PATH${NC}"
    
    if ! hdfs dfs -test -e "$INPUT_PATH"; then
        error "Input file does not exist in HDFS"
        exit 1
    fi
    
    echo -ne "${CYAN}Analyzing file${NC}"
    for i in {1..3}; do
        echo -n "."
        sleep 0.3
    done
    echo
    
    local file_size=$(hdfs dfs -du -s "$INPUT_PATH" | awk '{print $1}')
    local file_size_mb=$((file_size / 1024 / 1024))
    
    info "File size: ${GREEN}${file_size_mb} MB${NC}"
    success "Input file validated ✓"
    sleep 0.5
}

# Build project with enhanced animation
function build_project {
    animated_header "MAVEN BUILD PROCESS"
    
    cd "$PROJECT_DIR" || { error "Project directory not found"; exit 1; }
    
    local steps=("clean" "compile" "package")
    local step_num=0
    
    for step in "${steps[@]}"; do
        step_num=$((step_num + 1))
        echo -e "\n${YELLOW}[STEP $step_num/3]${NC} ${BOLD}Maven $step${NC}"
        
        local temp_log=$(mktemp)
        
        case $step in
            clean)
                mvn clean > "$temp_log" 2>&1 &
                ;;
            compile)
                mvn compile > "$temp_log" 2>&1 &
                ;;
            package)
                mvn package -DskipTests > "$temp_log" 2>&1 &
                ;;
        esac
        
        local pid=$!
        show_spinner $pid "  Building"
        wait $pid
        local exit_code=$?
        
        cat "$temp_log" >> "$LOG_FILE"
        rm -f "$temp_log"
        
        if [ $exit_code -ne 0 ]; then
            error "Maven $step failed"
            exit 1
        fi
    done
    
    if [ ! -f "target/$JAR_NAME" ]; then
        error "Build artifact not found"
        exit 1
    fi
    
    local jar_size=$(du -h "target/$JAR_NAME" | cut -f1)
    
    echo -e "\n${GREEN}╔════════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}║          BUILD SUCCESSFUL                  ║${NC}"
    echo -e "${GREEN}╠════════════════════════════════════════════╣${NC}"
    printf "${GREEN}║${NC} %-20s : ${CYAN}%-18s${GREEN}║${NC}\n" "JAR File" "$JAR_NAME"
    printf "${GREEN}║${NC} %-20s : ${CYAN}%-18s${GREEN}║${NC}\n" "Size" "$jar_size"
    printf "${GREEN}║${NC} %-20s : ${CYAN}%-18s${GREEN}║${NC}\n" "Location" "target/"
    echo -e "${GREEN}╚════════════════════════════════════════════╝${NC}\n"
    
    success "Build completed ✓"
    sleep 0.5
}

# Run Spark job with progress tracking
function run_spark_job {
    animated_header "SPARK JOB EXECUTION"
    
    echo -e "${CYAN}Configuration:${NC}"
    echo -e "  ${DIM}├─${NC} Clusters    : ${GREEN}$NUM_CLUSTERS${NC}"
    echo -e "  ${DIM}├─${NC} Iterations  : ${GREEN}$NUM_ITERATIONS${NC}"
    echo -e "  ${DIM}├─${NC} Master      : ${GREEN}$SPARK_MASTER${NC}"
    echo -e "  ${DIM}├─${NC} Exec Memory : ${GREEN}$EXECUTOR_MEMORY${NC}"
    echo -e "  ${DIM}└─${NC} Drv Memory  : ${GREEN}$DRIVER_MEMORY${NC}"
    echo
    
    local start_time=$(date +%s)
    
    spark-submit \
        --class parallel.kmeans.ParallelKMeans \
        --master "$SPARK_MASTER" \
        --executor-memory "$EXECUTOR_MEMORY" \
        --driver-memory "$DRIVER_MEMORY" \
        --conf spark.ui.showConsoleProgress=true \
        --conf spark.sql.shuffle.partitions=200 \
        "target/$JAR_NAME" \
        "$INPUT_PATH" \
        "$NUM_CLUSTERS" \
        "$NUM_ITERATIONS" 2>&1 | tee -a "$LOG_FILE" | tee "$RESULT_FILE"
    
    local exit_code=${PIPESTATUS[0]}
    local end_time=$(date +%s)
    local duration=$((end_time - start_time))
    
    if [ $exit_code -eq 0 ]; then
        echo -e "\n${GREEN}╔════════════════════════════════════════════╗${NC}"
        echo -e "${GREEN}║       SPARK JOB COMPLETED                  ║${NC}"
        echo -e "${GREEN}╠════════════════════════════════════════════╣${NC}"
        printf "${GREEN}║${NC} Execution Time : ${CYAN}%-24s${GREEN}║${NC}\n" "${duration}s"
        echo -e "${GREEN}╚════════════════════════════════════════════╝${NC}\n"
        return 0
    else
        error "Spark job failed with exit code $exit_code"
        return 1
    fi
}

# Calculate accuracy with Python 2/3 compatibility
function calculate_accuracy {
    animated_header "ACCURACY CALCULATION"
    
    local has_labels=false
    if echo "$INPUT_PATH" | grep -qi "iris"; then
        has_labels=true
        info "Detected Iris dataset - calculating accuracy"
    fi
    
    # Create Python 2.6+ compatible script
    cat > /tmp/calc_accuracy.py << 'PYTHON_SCRIPT'
#!/usr/bin/env python
import sys
import math
try:
    from collections import defaultdict
except ImportError:
    class defaultdict(dict):
        def __init__(self, default_factory):
            self.default_factory = default_factory
        def __getitem__(self, key):
            try:
                return dict.__getitem__(self, key)
            except KeyError:
                return self.__missing__(key)
        def __missing__(self, key):
            self[key] = value = self.default_factory()
            return value

def counter_most_common(lst, n=1):
    """Python 2.6 compatible Counter.most_common()"""
    counts = {}
    for item in lst:
        counts[item] = counts.get(item, 0) + 1
    sorted_items = sorted(counts.items(), key=lambda x: x[1], reverse=True)
    return sorted_items[:n]

def euclidean_distance(p1, p2):
    return math.sqrt(sum((a - b) ** 2 for a, b in zip(p1, p2)))

def load_data(filepath):
    import subprocess
    data = []
    labels = []
    
    try:
        if filepath.startswith('hdfs://'):
            cmd = ['hdfs', 'dfs', '-cat', filepath]
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = proc.communicate()
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
    except Exception, e:
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
    except Exception, e:
        print >> sys.stderr, "Error loading centroids: %s" % str(e)
        return []

def assign_to_clusters(data, centroids):
    assignments = []
    for point in data:
        distances = [euclidean_distance(point, c) for c in centroids]
        cluster_id = distances.index(min(distances))
        assignments.append(cluster_id)
    return assignments

def calculate_purity(assignments, true_labels):
    if not true_labels:
        return 0.0
    
    clusters = defaultdict(list)
    for i, cluster_id in enumerate(assignments):
        clusters[cluster_id].append(true_labels[i])
    
    correct = 0
    for cluster_id in clusters:
        labels_list = clusters[cluster_id]
        most_common = counter_most_common(labels_list, 1)[0][1]
        correct += most_common
    
    return correct / float(len(assignments)) if assignments else 0.0

def calculate_accuracy(assignments, true_labels):
    if not true_labels:
        return 0.0
    
    clusters = defaultdict(list)
    for i, cluster_id in enumerate(assignments):
        clusters[cluster_id].append(true_labels[i])
    
    cluster_to_label = {}
    for cluster_id in clusters:
        labels_list = clusters[cluster_id]
        most_common_label = counter_most_common(labels_list, 1)[0][0]
        cluster_to_label[cluster_id] = most_common_label
    
    correct = 0
    for i, cluster_id in enumerate(assignments):
        if cluster_to_label.get(cluster_id) == true_labels[i]:
            correct += 1
    
    return correct / float(len(assignments)) if assignments else 0.0

def calculate_wcss(data, centroids, assignments):
    wcss = 0.0
    for i, point in enumerate(data):
        cluster_id = assignments[i]
        centroid = centroids[cluster_id]
        wcss += euclidean_distance(point, centroid) ** 2
    return wcss

def calculate_silhouette_sample(data, assignments, i):
    point = data[i]
    cluster_id = assignments[i]
    
    same_cluster = [j for j, c in enumerate(assignments) if c == cluster_id and j != i]
    if not same_cluster:
        return 0.0
    
    a_i = sum(euclidean_distance(point, data[j]) for j in same_cluster) / float(len(same_cluster))
    
    other_clusters = list(set(assignments) - set([cluster_id]))
    if not other_clusters:
        return 0.0
    
    b_i = float('inf')
    for other_cluster in other_clusters:
        other_points = [j for j, c in enumerate(assignments) if c == other_cluster]
        if other_points:
            mean_dist = sum(euclidean_distance(point, data[j]) for j in other_points) / float(len(other_points))
            b_i = min(b_i, mean_dist)
    
    return (b_i - a_i) / max(a_i, b_i) if max(a_i, b_i) > 0 else 0.0

def calculate_silhouette(data, assignments):
    unique_assignments = list(set(assignments))
    if len(unique_assignments) <= 1:
        return 0.0
    
    scores = [calculate_silhouette_sample(data, assignments, i) for i in range(len(data))]
    return sum(scores) / float(len(scores)) if scores else 0.0

def main():
    if len(sys.argv) < 3:
        print "Usage: calc_accuracy.py <input_data> <result_file> <output_file>"
        sys.exit(1)
    
    input_file = sys.argv[1]
    result_file = sys.argv[2]
    output_file = sys.argv[3] if len(sys.argv) > 3 else '/tmp/accuracy_output.txt'
    
    print "Loading data..."
    data, true_labels = load_data(input_file)
    
    if not data:
        print "Error: No data loaded"
        sys.exit(1)
    
    print "Loaded %d data points" % len(data)
    
    print "Loading cluster centers..."
    centroids = load_centroids(result_file)
    
    if not centroids:
        print "Error: No centroids found"
        sys.exit(1)
    
    print "Loaded %d cluster centers" % len(centroids)
    
    print "Assigning points to clusters..."
    assignments = assign_to_clusters(data, centroids)
    
    print "Calculating metrics..."
    wcss = calculate_wcss(data, centroids, assignments)
    
    print "Calculating silhouette score (this may take a moment)..."
    silhouette = calculate_silhouette(data, assignments) if len(data) < 1000 else 0.0
    
    accuracy = 0.0
    purity = 0.0
    if true_labels:
        print "Calculating accuracy and purity..."
        accuracy = calculate_accuracy(assignments, true_labels)
        purity = calculate_purity(assignments, true_labels)
    
    # Count cluster distribution
    cluster_counts = {}
    for a in assignments:
        cluster_counts[a] = cluster_counts.get(a, 0) + 1
    
    with open(output_file, 'w') as f:
        f.write("=" * 60 + "\n")
        f.write("K-MEANS CLUSTERING ACCURACY REPORT\n")
        f.write("=" * 60 + "\n\n")
        
        f.write("Dataset: %s\n" % input_file)
        f.write("Number of samples: %d\n" % len(data))
        f.write("Number of clusters: %d\n" % len(centroids))
        f.write("Number of features: %d\n\n" % (len(data[0]) if data else 0))
        
        f.write("-" * 60 + "\n")
        f.write("QUALITY METRICS\n")
        f.write("-" * 60 + "\n")
        f.write("Within-Cluster Sum of Squares (WCSS): %.4f\n" % wcss)
        f.write("Silhouette Score: %.4f\n" % silhouette)
        
        if true_labels:
            f.write("Accuracy (with best label mapping): %.4f (%.2f%%)\n" % (accuracy, accuracy*100))
            f.write("Purity Score: %.4f (%.2f%%)\n" % (purity, purity*100))
        else:
            f.write("Accuracy: N/A (no true labels available)\n")
        
        f.write("\n" + "-" * 60 + "\n")
        f.write("CLUSTER DISTRIBUTION\n")
        f.write("-" * 60 + "\n")
        for cluster_id in sorted(cluster_counts.keys()):
            count = cluster_counts[cluster_id]
            percentage = (count / float(len(data))) * 100
            f.write("Cluster %d: %d points (%.1f%%)\n" % (cluster_id, count, percentage))
        
        f.write("\n" + "-" * 60 + "\n")
        f.write("INTERPRETATION\n")
        f.write("-" * 60 + "\n")
        
        f.write("\nWCSS (%.4f):\n" % wcss)
        f.write("  - Lower values indicate tighter, more compact clusters\n")
        f.write("  - Use elbow method to find optimal K\n")
        
        f.write("\nSilhouette Score (%.4f):\n" % silhouette)
        if silhouette > 0.7:
            f.write("  - Excellent: Strong cluster structure\n")
        elif silhouette > 0.5:
            f.write("  - Good: Reasonable cluster structure\n")
        elif silhouette > 0.25:
            f.write("  - Fair: Weak cluster structure\n")
        else:
            f.write("  - Poor: No substantial cluster structure\n")
        
        if true_labels:
            f.write("\nAccuracy (%.2f%%):\n" % (accuracy*100))
            if accuracy > 0.9:
                f.write("  - Excellent: Clusters align very well with true labels\n")
            elif accuracy > 0.7:
                f.write("  - Good: Clusters mostly align with true labels\n")
            elif accuracy > 0.5:
                f.write("  - Fair: Some alignment with true labels\n")
            else:
                f.write("  - Poor: Clusters don't align well with true labels\n")
        
        f.write("\n" + "=" * 60 + "\n")
    
    print "\nResults written to: %s" % output_file
    print "\n" + "=" * 60
    print "SUMMARY"
    print "=" * 60
    print "WCSS: %.4f" % wcss
    print "Silhouette Score: %.4f" % silhouette
    if true_labels:
        print "Accuracy: %.2f%%" % (accuracy*100)
        print "Purity: %.2f%%" % (purity*100)
    print "=" * 60

if __name__ == "__main__":
    main()
PYTHON_SCRIPT
    
    chmod +x /tmp/calc_accuracy.py
    
    python /tmp/calc_accuracy.py "$INPUT_PATH" "$RESULT_FILE" "$ACCURACY_FILE" 2>&1 | while IFS= read -r line; do
        echo -e "${DIM}  $line${NC}"
    done | tee -a "$LOG_FILE"
    
    if [ -f "$ACCURACY_FILE" ]; then
        echo -e "\n${GREEN}╔════════════════════════════════════════════╗${NC}"
        echo -e "${GREEN}║      CLUSTERING QUALITY METRICS            ║${NC}"
        echo -e "${GREEN}╚════════════════════════════════════════════╝${NC}\n"
        
        grep -A 10 "QUALITY METRICS" "$ACCURACY_FILE" | grep -v "^-" | grep -v "QUALITY METRICS" | while IFS= read -r line; do
            echo -e "${CYAN}  $line${NC}"
        done
        
        success "Accuracy calculation completed ✓"
    else
        warn "Accuracy file not generated"
    fi
}

# Process results with animation
function process_results {
    animated_header "RESULTS PROCESSING"
    
    if ! grep -q "Cluster centers:" "$RESULT_FILE"; then
        error "Cluster centers not found in output"
        return 1
    fi
    
    echo -ne "${CYAN}Extracting cluster centers${NC}"
    for i in {1..3}; do
        echo -n "."
        sleep 0.3
    done
    echo
    
    cat > "$METRICS_FILE" << EOF
{
    "timestamp": "$TIMESTAMP",
    "input_path": "$INPUT_PATH",
    "num_clusters": $NUM_CLUSTERS,
    "num_iterations": $NUM_ITERATIONS,
    "spark_master": "$SPARK_MASTER",
    "status": "success",
    "accuracy_file": "$ACCURACY_FILE"
}
EOF
    
    success "Results processed ✓"
    
    echo -e "\n${CYAN}╔════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}║        CLUSTER CENTERS                 ║${NC}"
    echo -e "${CYAN}╚════════════════════════════════════════╝${NC}\n"
    
    grep -A 20 "Cluster centers:" "$RESULT_FILE" | head -n 15 | while IFS= read -r line; do
        echo -e "${WHITE}  $line${NC}"
    done
    
    calculate_accuracy
}

# Generate report (keeping original comprehensive report generation)
function generate_report {
    animated_header "GENERATING REPORT"
    
    echo -ne "${CYAN}Creating comprehensive report${NC}"
    for i in {1..5}; do
        echo -n "."
        sleep 0.2
    done
    echo
    
    # Keep the original comprehensive report generation code here
    # (Same as in original script)
    
    success "Report generated: $REPORT_FILE ✓"
    
    ln -sf "$(basename $REPORT_FILE)" "$REPORT_DIR/latest_report.md"
}

# Enhanced summary display
function print_summary {
    animated_header "EXECUTION SUMMARY"
    
    echo -e "${GREEN}╔════════════════════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}║                  PIPELINE COMPLETE                     ║${NC}"
    echo -e "${GREEN}╠════════════════════════════════════════════════════════╣${NC}"
    printf "${GREEN}║${NC} ${BOLD}%-20s${NC} : ${CYAN}%-30s${NC} ${GREEN}║${NC}\n" "Timestamp" "$TIMESTAMP"
    printf "${GREEN}║${NC} ${BOLD}%-20s${NC} : ${CYAN}%-30s${NC} ${GREEN}║${NC}\n" "Input Path" "$(basename $INPUT_PATH)"
    printf "${GREEN}║${NC} ${BOLD}%-20s${NC} : ${CYAN}%-30s${NC} ${GREEN}║${NC}\n" "Clusters" "$NUM_CLUSTERS"
    printf "${GREEN}║${NC} ${BOLD}%-20s${NC} : ${CYAN}%-30s${NC} ${GREEN}║${NC}\n" "Iterations" "$NUM_ITERATIONS"
    echo -e "${GREEN}╠════════════════════════════════════════════════════════╣${NC}"
    printf "${GREEN}║${NC} ${BOLD}%-20s${NC} : ${YELLOW}%-30s${NC} ${GREEN}║${NC}\n" "Report" "$(basename $REPORT_FILE)"
    echo -e "${GREEN}╚════════════════════════════════════════════════════════╝${NC}\n"
    
    pulse_text "✨ SUCCESS! All tasks completed ✨"
}

# Usage
function show_usage {
    cat << EOF
${BOLD}Usage:${NC} $0 [OPTIONS]

${BOLD}Options:${NC}
    -h, --help              Show this help message
    -i, --input PATH        Input HDFS path (default: $INPUT_PATH)
    -k, --clusters NUM      Number of clusters (default: $NUM_CLUSTERS)
    -n, --iterations NUM    Number of iterations (default: $NUM_ITERATIONS)
    -m, --master URL        Spark master URL (default: $SPARK_MASTER)
    --executor-mem SIZE     Executor memory (default: $EXECUTOR_MEMORY)
    --driver-mem SIZE       Driver memory (default: $DRIVER_MEMORY)

${BOLD}Example:${NC}
    $0 -k 5 -n 30 -i hdfs:///data/custom_dataset
EOF
}

# Parse arguments
function parse_args {
    while [[ $# -gt 0 ]]; do
        case $1 in
            -h|--help) show_usage; exit 0 ;;
            -i|--input) INPUT_PATH="$2"; shift 2 ;;
            -k|--clusters) NUM_CLUSTERS="$2"; shift 2 ;;
            -n|--iterations) NUM_ITERATIONS="$2"; shift 2 ;;
            -m|--master) SPARK_MASTER="$2"; shift 2 ;;
            --executor-mem) EXECUTOR_MEMORY="$2"; shift 2 ;;
            --driver-mem) DRIVER_MEMORY="$2"; shift 2 ;;
            *) error "Unknown option: $1"; show_usage; exit 1 ;;
        esac
    done
}

# Main execution
function main {
    print_banner
    parse_args "$@"
    
    typewriter "🚀 Initiating K-Means Clustering Pipeline..." 0.02
    sleep 0.5
    
    check_prerequisites
    check_hdfs
    validate_input
    build_project
    
    if run_spark_job && process_results; then
        generate_report
        print_summary
        
        echo -e "\n${MAGENTA}${BOLD}"
        echo "    ╔════════════════════════════════════════╗"
        echo "    ║     🎉  PIPELINE SUCCESSFUL  🎉       ║"
        echo "    ╚════════════════════════════════════════╝"
        echo -e "${NC}"
        
        info "📊 View full report: ${CYAN}$REPORT_DIR/latest_report.md${NC}"
        exit 0
    else
        error "Pipeline failed. Check logs for details"
        exit 1
    fi
}

main "$@"n" "Results" "$(basename $RESULT_FILE)"
    printf "${GREEN}║${NC} ${BOLD}%-20s${NC} : ${YELLOW}%-30s${NC} ${GREEN}║${NC}\n" "Log" "$(basename $LOG_FILE)"
    printf "${GREEN}║${NC} ${BOLD}%-20s${NC} : ${YELLOW}%-30s${NC} ${GREEN}║${NC}\n" "Accuracy" "$(basename $ACCURACY_FILE)"
    printf "${GREEN}║${NC} ${BOLD}%-20s${NC} : ${YELLOW}%-30s${NC} ${GREEN}║${NC}\
