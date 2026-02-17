#!/usr/bin/env python3
"""
Test script for the schema evaluation framework.

This script tests the evaluation components to ensure they work correctly.
"""

import sys
import json
from pathlib import Path

# Test imports
try:
    from .few_shot_manager import FewShotManager
    from .gt_comparator import GTComparator
    print("✅ Imports successful")
except ImportError as e:
    print(f"❌ Import failed: {e}")
    sys.exit(1)

def test_few_shot_manager():
    """Test FewShotManager functionality."""
    print("\n🧪 Testing FewShotManager...")

    # Sample test data
    test_data = [
        {"paper_name": "paper1", "ground_truth": "yes", "protein_sequence": "ABCDEF"},
        {"paper_name": "paper2", "ground_truth": "no", "protein_sequence": "GHIJKL"},
        {"paper_name": "paper3", "ground_truth": "yes", "protein_sequence": "MNOPQR"},
    ]

    config = {
        "n_shots_per_category": 1,
        "selection_strategy": "stratified"
    }

    fsm = FewShotManager(config)
    examples = fsm.extract_gt_examples(test_data, "ground_truth")

    print(f"  Extracted {len(examples)} examples")

    formatted = fsm.format_examples_for_prompt(examples, "Does this protein have NES?", "ground_truth")
    print(f"  Formatted examples length: {len(formatted)} chars")

    if examples:
        print("✅ FewShotManager test passed")
        return True
    else:
        print("❌ FewShotManager test failed")
        return False

def test_gt_comparator():
    """Test GTComparator functionality."""
    print("\n🧪 Testing GTComparator...")

    comparator = GTComparator()

    # Test different comparison types
    tests = [
        ("yes", "yes", True),  # Exact match
        ("no", "yes", False),  # No match
        ("Yes, it has NES sequence", "yes", True),  # Contains match
        ("123", "123.0", True),  # Numeric match
    ]

    all_passed = True
    for predicted, gt, expected_match in tests:
        result = comparator.compare_answers(predicted, gt)
        if result.exact_match != expected_match:
            print(f"❌ Failed: {predicted} vs {gt} - Expected {expected_match}, got {result.exact_match}")
            all_passed = False

    if all_passed:
        print("✅ GTComparator test passed")
        return True
    else:
        print("❌ GTComparator test failed")
        return False

def test_config_loading():
    """Test configuration file loading."""
    print("\n🧪 Testing configuration loading...")

    config_path = Path(__file__).parent / "config" / "nes_evaluation_config.json"

    if not config_path.exists():
        print(f"❌ Config file not found: {config_path}")
        return False

    try:
        with open(config_path, 'r') as f:
            config = json.load(f)

        required_keys = ["gt_column", "few_shot_config", "evaluator_config"]
        for key in required_keys:
            if key not in config:
                print(f"❌ Missing required config key: {key}")
                return False

        print("✅ Configuration loading test passed")
        return True

    except Exception as e:
        print(f"❌ Configuration loading failed: {e}")
        return False

def main():
    """Run all tests."""
    print("🚀 Running Schema Evaluation Framework Tests")
    print("=" * 50)

    tests = [
        test_few_shot_manager,
        test_gt_comparator,
        test_config_loading
    ]

    results = []
    for test in tests:
        try:
            results.append(test())
        except Exception as e:
            print(f"❌ Test {test.__name__} crashed: {e}")
            results.append(False)

    # Summary
    passed = sum(results)
    total = len(results)

    print(f"\n📊 TEST SUMMARY")
    print(f"=" * 30)
    print(f"Passed: {passed}/{total}")
    print(f"Success Rate: {passed/total*100:.1f}%")

    if passed == total:
        print("🎉 All tests passed!")
        return 0
    else:
        print("⚠️  Some tests failed")
        return 1

if __name__ == "__main__":
    sys.exit(main())
