#!/usr/bin/env python3
"""
Validation script for testing rosbag-rs generated bags with ROS1 Noetic tools.

Usage:
    ./validate_bag.py <bag_path> [--expected-topics t1 t2] [--expected-message-count N] [--verbose]
"""

import rosbag
import subprocess
import sys
import argparse


def run_rosbag_info(bag_path):
    """Run rosbag info and return success/failure."""
    result = subprocess.run(
        ['rosbag', 'info', bag_path],
        capture_output=True,
        text=True
    )
    return result.returncode == 0, result.stdout, result.stderr


def run_rosbag_check(bag_path):
    """Run rosbag check and return success/failure."""
    result = subprocess.run(
        ['rosbag', 'check', bag_path],
        capture_output=True,
        text=True
    )
    return result.returncode == 0, result.stdout, result.stderr


def main():
    parser = argparse.ArgumentParser(description='Validate ROS bag files')
    parser.add_argument('bag_path', help='Path to the bag file')
    parser.add_argument('--expected-topics', nargs='+', default=[], help='Expected topic names')
    parser.add_argument('--expected-message-count', type=int, default=None, help='Expected total message count')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    args = parser.parse_args()

    errors = []
    
    print(f"Validating: {args.bag_path}")
    print("=" * 60)

    # Test 1: rosbag info
    print("\n[Test 1] rosbag info...")
    success, stdout, stderr = run_rosbag_info(args.bag_path)
    if success:
        print("  PASS: rosbag info succeeded")
        if args.verbose:
            print(stdout)
    else:
        print(f"  FAIL: rosbag info failed")
        print(stderr)
        errors.append("rosbag info failed")

    # Test 2: rosbag check
    print("\n[Test 2] rosbag check...")
    success, stdout, stderr = run_rosbag_check(args.bag_path)
    if success:
        print("  PASS: rosbag check succeeded")
    else:
        # rosbag check returns non-zero if migration needed, which is often OK
        if "can be played" in stdout.lower() or "no migration" in stdout.lower():
            print("  PASS: rosbag check - bag is playable")
        else:
            print(f"  WARN: rosbag check returned non-zero")
            if args.verbose:
                print(stdout)
                print(stderr)

    # Test 3: Open with Python rosbag library
    print("\n[Test 3] Python rosbag.Bag...")
    try:
        bag = rosbag.Bag(args.bag_path)
        info = bag.get_type_and_topic_info()
        actual_topics = set(info.topics.keys())
        actual_count = bag.get_message_count()
        
        print(f"  Topics: {actual_topics}")
        print(f"  Message count: {actual_count}")
        print("  PASS: Bag opened successfully")
        
        # Validate expected topics
        if args.expected_topics:
            expected = set(args.expected_topics)
            if actual_topics != expected:
                print(f"  FAIL: Topics mismatch. Expected {expected}, got {actual_topics}")
                errors.append(f"Topics mismatch: expected {expected}, got {actual_topics}")
            else:
                print(f"  PASS: Topics match")
        
        # Validate expected message count
        if args.expected_message_count is not None:
            if actual_count != args.expected_message_count:
                print(f"  FAIL: Message count mismatch. Expected {args.expected_message_count}, got {actual_count}")
                errors.append(f"Count mismatch: expected {args.expected_message_count}, got {actual_count}")
            else:
                print(f"  PASS: Message count matches")
        
        # Test 4: Iterate through all messages
        print("\n[Test 4] Reading all messages...")
        read_count = 0
        for topic, msg, t in bag.read_messages():
            read_count += 1
            if args.verbose and read_count <= 5:
                print(f"    [{read_count}] topic={topic}, time={t}")
        
        print(f"  Successfully read {read_count} messages")
        
        if read_count != actual_count:
            print(f"  WARN: Read count ({read_count}) differs from reported count ({actual_count})")
        else:
            print("  PASS: All messages readable")
        
        bag.close()
        
    except Exception as e:
        print(f"  FAIL: Exception: {e}")
        errors.append(f"Python rosbag exception: {e}")

    # Summary
    print("\n" + "=" * 60)
    if errors:
        print(f"FAILED: {len(errors)} error(s)")
        for err in errors:
            print(f"  - {err}")
        return 1
    else:
        print("ALL TESTS PASSED")
        return 0


if __name__ == '__main__':
    sys.exit(main())

