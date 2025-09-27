#!/usr/bin/env python3
"""
Test script to verify Kafka reinitialization works correctly
"""

import subprocess
import time
import sys


def run_kubectl(cmd, timeout=30):
    """Run a kubectl command and return the result"""
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return result.returncode == 0, result.stdout, result.stderr
    except Exception as e:
        return False, "", str(e)


def create_test_topic():
    """Create a test topic in Kafka"""
    print("[INFO] Creating test topic 'test-reinit'...")
    
    # Use kubectl exec to run kafka-topics.sh inside the pod
    cmd = [
        'kubectl', 'exec', '-n', 'globeco', 'globeco-execution-service-kafka-0', '--',
        '/opt/kafka/bin/kafka-topics.sh',
        '--bootstrap-server', 'localhost:9092',
        '--create',
        '--topic', 'test-reinit',
        '--partitions', '1',
        '--replication-factor', '1'
    ]
    
    success, stdout, stderr = run_kubectl(cmd)
    if success:
        print("[SUCCESS] Test topic created")
        return True
    else:
        print(f"[ERROR] Failed to create test topic: {stderr}")
        return False


def list_topics():
    """List all topics in Kafka"""
    print("[INFO] Listing Kafka topics...")
    
    cmd = [
        'kubectl', 'exec', '-n', 'globeco', 'globeco-execution-service-kafka-0', '--',
        '/opt/kafka/bin/kafka-topics.sh',
        '--bootstrap-server', 'localhost:9092',
        '--list'
    ]
    
    success, stdout, stderr = run_kubectl(cmd)
    if success:
        topics = [line.strip() for line in stdout.strip().split('\n') if line.strip()]
        print(f"[INFO] Found topics: {topics}")
        return topics
    else:
        print(f"[ERROR] Failed to list topics: {stderr}")
        return []


def test_kafka_reinit():
    """Test the complete Kafka reinitialization process"""
    print("=" * 60)
    print("TESTING KAFKA REINITIALIZATION")
    print("=" * 60)
    
    # Step 1: List initial topics
    print("\n1. Initial state:")
    initial_topics = list_topics()
    
    # Step 2: Create a test topic
    print("\n2. Creating test data:")
    if not create_test_topic():
        return False
    
    # Step 3: Verify test topic exists
    print("\n3. Verifying test topic exists:")
    topics_with_test = list_topics()
    if 'test-reinit' not in topics_with_test:
        print("[ERROR] Test topic was not created successfully")
        return False
    print("[SUCCESS] Test topic confirmed to exist")
    
    # Step 4: Run reinitialization
    print("\n4. Running Kafka reinitialization:")
    print("-" * 40)
    result = subprocess.run(['python3', 'k8s/kafka_reinit_kubectl.py'], 
                          capture_output=False, text=True)
    print("-" * 40)
    
    if result.returncode != 0:
        print("[ERROR] Reinitialization failed")
        return False
    
    # Step 5: Wait a moment for Kafka to be fully ready
    print("\n5. Waiting for Kafka to be fully ready...")
    time.sleep(10)
    
    # Step 6: Verify test topic is gone
    print("\n6. Verifying test topic is gone:")
    final_topics = list_topics()
    if 'test-reinit' in final_topics:
        print("[ERROR] Test topic still exists after reinitialization!")
        return False
    
    print("[SUCCESS] Test topic successfully removed by reinitialization")
    
    # Step 7: Compare topic lists
    print("\n7. Topic comparison:")
    print(f"   Initial topics: {initial_topics}")
    print(f"   Final topics:   {final_topics}")
    
    # Should only have system topics
    system_topics = [t for t in final_topics if t.startswith('__')]
    user_topics = [t for t in final_topics if not t.startswith('__')]
    
    if user_topics:
        print(f"[WARNING] Found unexpected user topics after reinit: {user_topics}")
    else:
        print("[SUCCESS] Only system topics remain after reinitialization")
    
    print("\n" + "=" * 60)
    print("KAFKA REINITIALIZATION TEST COMPLETED SUCCESSFULLY!")
    print("=" * 60)
    return True


if __name__ == "__main__":
    success = test_kafka_reinit()
    sys.exit(0 if success else 1)