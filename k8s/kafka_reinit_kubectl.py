#!/usr/bin/env python3
"""
Kafka Reinitialization using kubectl commands
This module quickly reinitializes Kafka to ensure each test starts from the same clean state
"""

import time
import subprocess
import json
import sys


def run_kubectl(cmd, timeout=30):
    """Run a kubectl command and return the result"""
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return result.returncode == 0, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        return False, "", "Command timed out"
    except Exception as e:
        return False, "", str(e)


def wait_for_pod_termination(pod_name, namespace, timeout=60):
    """Wait for a pod to be terminated"""
    print(f"[INFO] Waiting for pod {pod_name} to terminate...")
    
    for _ in range(timeout):
        success, stdout, stderr = run_kubectl(['kubectl', 'get', 'pod', pod_name, '-n', namespace, '-o', 'json'])
        if not success:
            # Pod doesn't exist, so it's terminated
            print(f"[SUCCESS] Pod {pod_name} terminated")
            return True
        
        time.sleep(1)
    
    print(f"[ERROR] Timeout waiting for pod {pod_name} to terminate")
    return False


def wait_for_pod_ready(pod_name, namespace, timeout=180):
    """Wait for a pod to be ready"""
    print(f"[INFO] Waiting for pod {pod_name} to be ready...")
    
    for _ in range(timeout // 2):
        success, stdout, stderr = run_kubectl(['kubectl', 'get', 'pod', pod_name, '-n', namespace, '-o', 'json'])
        if success:
            try:
                pod_data = json.loads(stdout)
                conditions = pod_data.get('status', {}).get('conditions', [])
                ready_condition = next((c for c in conditions if c['type'] == 'Ready'), None)
                if ready_condition and ready_condition['status'] == 'True':
                    print(f"[SUCCESS] Pod {pod_name} is ready")
                    return True
            except:
                pass
        
        time.sleep(2)
    
    print(f"[ERROR] Timeout waiting for pod {pod_name} to be ready")
    return False


def clean_kafka_directory(namespace, node_name="node-3"):
    """Clean the Kafka data directory using a privileged cleanup pod"""
    print(f"[INFO] Cleaning Kafka data directory on {node_name}...")
    
    cleanup_pod_yaml = f"""
apiVersion: v1
kind: Pod
metadata:
  name: kafka-cleanup-temp
  namespace: {namespace}
spec:
  restartPolicy: Never
  nodeSelector:
    kubernetes.io/hostname: {node_name}
  hostPID: true
  hostNetwork: true
  securityContext:
    runAsUser: 0
    runAsGroup: 0
    fsGroup: 0
  containers:
  - name: cleanup
    image: busybox
    securityContext:
      privileged: true
      runAsUser: 0
    command:
    - /bin/sh
    - -c
    - |
      echo "Cleaning Kafka data directory..."
      cd /mnt/disk1/kafka-execution-service
      rm -rf * .[!.]* 2>/dev/null || true
      echo "Directory cleaned"
      chown -R 1000:1000 /mnt/disk1/kafka-execution-service
      echo "Permissions set"
    volumeMounts:
    - name: kafka-data
      mountPath: /mnt/disk1/kafka-execution-service
  volumes:
  - name: kafka-data
    hostPath:
      path: /mnt/disk1/kafka-execution-service
      type: Directory
"""
    
    try:
        # Create cleanup pod
        with open('/tmp/kafka-cleanup.yaml', 'w') as f:
            f.write(cleanup_pod_yaml)
        
        success, stdout, stderr = run_kubectl(['kubectl', 'apply', '-f', '/tmp/kafka-cleanup.yaml'])
        if not success:
            print(f"[ERROR] Failed to create cleanup pod: {stderr}")
            return False
        
        # Wait for pod to complete
        print("[INFO] Waiting for cleanup to complete...")
        for _ in range(60):
            success, stdout, stderr = run_kubectl(['kubectl', 'get', 'pod', 'kafka-cleanup-temp', 
                                                 '-n', namespace, '-o', 'jsonpath={.status.phase}'])
            if success:
                phase = stdout.strip()
                if phase == 'Succeeded':
                    print("[SUCCESS] Cleanup completed")
                    break
                elif phase == 'Failed':
                    print("[ERROR] Cleanup pod failed")
                    return False
            time.sleep(1)
        else:
            print("[ERROR] Timeout waiting for cleanup to complete")
            return False
        
        # Clean up the cleanup pod
        run_kubectl(['kubectl', 'delete', 'pod', 'kafka-cleanup-temp', '-n', namespace])
        
        return True
        
    except Exception as e:
        print(f"[ERROR] Failed to clean node directory: {e}")
        return False


def reinitialize_kafka(namespace="globeco", 
                      statefulset_name="globeco-execution-service-kafka",
                      node_name="node-3",
                      pv_name="execution-service-kafka-pv",
                      pv_manifest_path="k8s/kafka-persistent-volume.yaml"):
    """
    Reinitialize Kafka by scaling down, cleaning data, resetting PV, and scaling back up
    
    Returns:
        bool: True if successful, False otherwise
    """
    print("[INFO] Starting Kafka reinitialization for benchmark testing...")
    
    pod_name = f"{statefulset_name}-0"
    pvc_name = f"kafka-data-{statefulset_name}-0"
    
    try:
        # Step 1: Scale StatefulSet to 0
        print("[INFO] Scaling StatefulSet to 0...")
        success, stdout, stderr = run_kubectl(['kubectl', 'scale', 'statefulset', statefulset_name, 
                                             '--replicas=0', '-n', namespace])
        if not success:
            print(f"[ERROR] Failed to scale StatefulSet: {stderr}")
            return False
        
        # Wait for pod to terminate
        if not wait_for_pod_termination(pod_name, namespace):
            return False
        
        # Step 2: Delete the PVC (this will put PV in Released state)
        print("[INFO] Deleting PVC...")
        success, stdout, stderr = run_kubectl(['kubectl', 'delete', 'pvc', pvc_name, '-n', namespace])
        if not success:
            print(f"[WARNING] Failed to delete PVC (might not exist): {stderr}")
        
        # Wait a moment for PVC deletion to complete
        time.sleep(5)
        
        # Step 3: Delete the PV to reset its state
        print("[INFO] Deleting PV to reset state...")
        success, stdout, stderr = run_kubectl(['kubectl', 'delete', 'pv', pv_name])
        if not success:
            print(f"[WARNING] Failed to delete PV: {stderr}")
        
        # Wait for PV deletion
        print("[INFO] Waiting for PV deletion...")
        for _ in range(30):
            success, stdout, stderr = run_kubectl(['kubectl', 'get', 'pv', pv_name])
            if not success:
                print("[SUCCESS] PV deleted")
                break
            time.sleep(1)
        else:
            print("[WARNING] PV deletion timeout, continuing anyway...")
        
        # Step 4: Clean the data directory
        if not clean_kafka_directory(namespace, node_name):
            return False
        
        # Step 5: Recreate the PV
        print("[INFO] Recreating PV...")
        success, stdout, stderr = run_kubectl(['kubectl', 'apply', '-f', pv_manifest_path])
        if not success:
            print(f"[ERROR] Failed to recreate PV: {stderr}")
            return False
        
        # Wait for PV to be available
        print("[INFO] Waiting for PV to be available...")
        for _ in range(30):
            success, stdout, stderr = run_kubectl(['kubectl', 'get', 'pv', pv_name, '-o', 'jsonpath={.status.phase}'])
            if success and stdout.strip() == 'Available':
                print("[SUCCESS] PV is available")
                break
            time.sleep(1)
        else:
            print("[WARNING] PV availability timeout, continuing anyway...")
        
        # Step 6: Scale StatefulSet back to 1
        print("[INFO] Scaling StatefulSet back to 1...")
        success, stdout, stderr = run_kubectl(['kubectl', 'scale', 'statefulset', statefulset_name, 
                                             '--replicas=1', '-n', namespace])
        if not success:
            print(f"[ERROR] Failed to scale StatefulSet: {stderr}")
            return False
        
        # Step 7: Wait for pod to be ready
        if not wait_for_pod_ready(pod_name, namespace):
            return False
        
        print("[SUCCESS] Kafka reinitialization completed successfully!")
        print(f"[SUCCESS] Kafka is ready at: {statefulset_name}.{namespace}.svc.cluster.local:9092")
        
        # Show final status
        print("\n[INFO] Final status:")
        success, stdout, stderr = run_kubectl(['kubectl', 'get', 'pv', pv_name, '-o', 'wide'])
        if success:
            print(f"PV Status: {stdout}")
        
        success, stdout, stderr = run_kubectl(['kubectl', 'get', 'pvc', pvc_name, '-n', namespace, '-o', 'wide'])
        if success:
            print(f"PVC Status: {stdout}")
        
        return True
        
    except Exception as e:
        print(f"[ERROR] Reinitialization failed: {e}")
        return False


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] in ["-h", "--help", "help"]:
        print("Usage: python kafka_reinit_kubectl.py")
        print()
        print("This script reinitializes Kafka by:")
        print("1. Scaling the StatefulSet to 0 (stops Kafka)")
        print("2. Deleting PVC and PV to reset state")
        print("3. Cleaning the data directory on the node")
        print("4. Recreating the PV")
        print("5. Scaling the StatefulSet back to 1 (fresh start)")
        print()
        print("This ensures each benchmark test starts with a completely clean Kafka instance.")
        print("The PV is deleted and recreated to avoid the 'Released' state issue.")
        sys.exit(0)
    
    success = reinitialize_kafka()
    sys.exit(0 if success else 1)