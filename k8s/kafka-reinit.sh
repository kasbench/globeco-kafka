#!/bin/bash

# Kafka Reinitialization Script for Microservice Benchmarks
# This script quickly reinitializes Kafka to ensure each test starts from the same clean state

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NAMESPACE="globeco"
STATEFULSET_NAME="globeco-execution-service-kafka"
PVC_NAME="kafka-data-globeco-execution-service-kafka-0"

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if kubectl is available
check_kubectl() {
    if ! command -v kubectl &> /dev/null; then
        print_error "kubectl is not installed or not in PATH"
        exit 1
    fi
}

# Function to wait for resource deletion
wait_for_deletion() {
    local resource_type=$1
    local resource_name=$2
    local namespace=$3
    local timeout=${4:-60}
    
    print_status "Waiting for $resource_type/$resource_name to be deleted..."
    
    local count=0
    while kubectl get "$resource_type" "$resource_name" -n "$namespace" &>/dev/null; do
        if [ $count -ge $timeout ]; then
            print_error "Timeout waiting for $resource_type/$resource_name deletion"
            return 1
        fi
        sleep 1
        ((count++))
    done
    
    print_success "$resource_type/$resource_name deleted"
    return 0
}

# Main reinitialization function
reinitialize_kafka() {
    print_status "Starting Kafka reinitialization for benchmark testing..."
    
    # Step 1: Delete the StatefulSet (this will delete the pod but keep PVC)
    print_status "Deleting StatefulSet to stop Kafka..."
    kubectl delete statefulset "$STATEFULSET_NAME" -n "$NAMESPACE" --ignore-not-found=true
    
    # Wait for pod to be terminated
    wait_for_deletion "pod" "${STATEFULSET_NAME}-0" "$NAMESPACE" 30
    
    # Step 2: Delete the PVC to completely wipe data
    print_status "Deleting PVC to wipe all Kafka data..."
    kubectl delete pvc "$PVC_NAME" -n "$NAMESPACE" --ignore-not-found=true
    
    # Wait for PVC deletion
    wait_for_deletion "pvc" "$PVC_NAME" "$NAMESPACE" 30
    
    # Step 3: Recreate the StatefulSet (this will create new PVC and reinitialize)
    print_status "Recreating StatefulSet with fresh storage..."
    kubectl apply -f "$SCRIPT_DIR/statefulset.yaml"
    
    # Step 4: Wait for new PVC to be created and bound
    print_status "Waiting for new PVC to be created and bound..."
    kubectl wait --for=condition=bound "pvc/$PVC_NAME" -n "$NAMESPACE" --timeout=60s
    
    # Step 5: Wait for Kafka pod to be ready
    print_status "Waiting for Kafka to be ready (this includes storage formatting)..."
    kubectl wait --for=condition=ready "pod/${STATEFULSET_NAME}-0" -n "$NAMESPACE" --timeout=180s
    
    print_success "Kafka reinitialization completed successfully!"
    
    # Show final status
    echo
    print_status "Current status:"
    kubectl get pod "${STATEFULSET_NAME}-0" -n "$NAMESPACE"
    kubectl get pvc "$PVC_NAME" -n "$NAMESPACE"
    
    echo
    print_success "Kafka is ready for benchmark testing at: ${STATEFULSET_NAME}.${NAMESPACE}.svc.cluster.local:9092"
}

# Function to show usage
show_usage() {
    echo "Usage: $0"
    echo
    echo "This script reinitializes Kafka by:"
    echo "1. Deleting the StatefulSet (stops Kafka)"
    echo "2. Deleting the PVC (wipes all data)"
    echo "3. Recreating the StatefulSet (fresh start with new storage)"
    echo
    echo "This ensures each benchmark test starts with a completely clean Kafka instance."
}

# Main script logic
main() {
    check_kubectl
    
    case "${1:-}" in
        -h|--help|help)
            show_usage
            exit 0
            ;;
        "")
            reinitialize_kafka
            ;;
        *)
            print_error "Unknown argument: $1"
            show_usage
            exit 1
            ;;
    esac
}

main "$@"