#!/bin/bash

# Kafka 4.0 Deployment Script for Kubernetes
# Usage: ./kafka-deploy.sh [apply|delete]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NAMESPACE="globeco"

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
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

# Function to wait for resource to be ready
wait_for_resource() {
    local resource_type=$1
    local resource_name=$2
    local namespace=$3
    local timeout=${4:-300}
    
    print_status "Waiting for $resource_type/$resource_name to be ready..."
    
    if kubectl wait --for=condition=ready "$resource_type/$resource_name" \
        --namespace="$namespace" --timeout="${timeout}s" 2>/dev/null; then
        print_success "$resource_type/$resource_name is ready"
        return 0
    else
        print_warning "$resource_type/$resource_name is not ready within ${timeout}s"
        return 1
    fi
}

# Function to apply Kafka resources
apply_kafka() {
    print_status "Deploying Kafka 4.0 to Kubernetes..."
    
    
    print_status "Creating ConfigMap..."
    kubectl apply -f "$SCRIPT_DIR/configmap.yaml"
    
    print_status "Creating headless service..."
    kubectl apply -f "$SCRIPT_DIR/service-headless.yaml"
    
    print_status "Creating service..."
    kubectl apply -f "$SCRIPT_DIR/service.yaml"
    
    print_status "Creating StatefulSet (with volumeClaimTemplates)..."
    kubectl apply -f "$SCRIPT_DIR/statefulset.yaml"
    
    # Wait for PVC to be created and bound by StatefulSet
    print_status "Waiting for StatefulSet PVC to be created and bound..."
    sleep 5  # Give time for PVC creation
    kubectl wait --for=condition=bound pvc/kafka-data-globeco-execution-service-kafka-0 \
        --namespace="$NAMESPACE" --timeout=60s || print_warning "PVC binding timeout"
    
    # Wait for StatefulSet to be ready
    print_status "Waiting for Kafka StatefulSet to be ready (this may take a few minutes)..."
    if kubectl wait --for=condition=ready pod/globeco-execution-service-kafka-0 \
        --namespace="$NAMESPACE" --timeout=300s; then
        print_success "Kafka deployment completed successfully!"
        
        # Show deployment status
        echo
        print_status "Deployment Status:"
        kubectl get pods,svc,pvc -n "$NAMESPACE" -l app=globeco-execution-service-kafka
        
        echo
        print_success "Kafka is accessible at: globeco-execution-service-kafka.globeco.svc.cluster.local:9092"
    else
        print_error "Kafka deployment failed or timed out"
        echo
        print_status "Pod logs:"
        kubectl logs -n "$NAMESPACE" globeco-execution-service-kafka-0 --tail=50 || true
        echo
        print_status "Init container logs:"
        kubectl logs -n "$NAMESPACE" globeco-execution-service-kafka-0 -c kafka-init --tail=50 || true
        exit 1
    fi
}

# Function to delete Kafka resources
delete_kafka() {
    print_status "Deleting Kafka 4.0 from Kubernetes..."
    
    # Delete resources in reverse order
    print_status "Deleting StatefulSet..."
    kubectl delete -f "$SCRIPT_DIR/statefulset.yaml" --ignore-not-found=true
    
    print_status "Deleting services..."
    kubectl delete -f "$SCRIPT_DIR/service.yaml" --ignore-not-found=true
    kubectl delete -f "$SCRIPT_DIR/service-headless.yaml" --ignore-not-found=true
    
    print_status "Deleting PersistentVolumeClaims (created by StatefulSet)..."
    kubectl delete pvc kafka-data-globeco-execution-service-kafka-0 -n "$NAMESPACE" --ignore-not-found=true
    
    print_status "Deleting ConfigMap..."
    kubectl delete -f "$SCRIPT_DIR/configmap.yaml" --ignore-not-found=true
    

    
    print_success "Kafka resources deleted successfully!"
}

# Function to show usage
show_usage() {
    echo "Usage: $0 [apply|delete|status]"
    echo
    echo "Commands:"
    echo "  apply   - Deploy Kafka to Kubernetes"
    echo "  delete  - Remove Kafka from Kubernetes"
    echo "  status  - Show current deployment status"
    echo
    echo "Examples:"
    echo "  $0 apply    # Deploy Kafka"
    echo "  $0 delete   # Remove Kafka"
    echo "  $0 status   # Check status"
}

# Function to show status
show_status() {
    print_status "Kafka Deployment Status:"
    echo
    
    if kubectl get namespace "$NAMESPACE" &>/dev/null; then
        print_success "Namespace '$NAMESPACE' exists"
        
        echo
        print_status "Resources in namespace '$NAMESPACE':"
        kubectl get all,pvc,configmap -n "$NAMESPACE" -l app=globeco-execution-service-kafka 2>/dev/null || \
            print_warning "No Kafka resources found"
        
        echo
        if kubectl get pod globeco-execution-service-kafka-0 -n "$NAMESPACE" &>/dev/null; then
            print_status "Pod status:"
            kubectl describe pod globeco-execution-service-kafka-0 -n "$NAMESPACE" | \
                grep -E "(Status:|Ready:|Restart Count:|Events:)" -A 5
        fi
    else
        print_warning "Namespace '$NAMESPACE' does not exist"
    fi
}

# Main script logic
main() {
    check_kubectl
    
    case "${1:-}" in
        apply)
            apply_kafka
            ;;
        delete)
            delete_kafka
            ;;
        status)
            show_status
            ;;
        *)
            show_usage
            exit 1
            ;;
    esac
}

main "$@"