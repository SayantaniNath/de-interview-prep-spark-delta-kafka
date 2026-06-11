# NeetCode 150 — Arrays & Hashing
# Run this file to verify your solutions: python3 neetcode_practice.py

from collections import defaultdict


# ============================================================
# 1. Two Sum
# ============================================================
# Problem: Given nums and target, return indices of the two numbers that add up to target.
#          Exactly one solution exists. Cannot use the same element twice.
# Example: nums = [2, 7, 11, 15], target = 9 → [0, 1]
# Time: O(n) | Space: O(n)

def two_sum(nums, target):
    seen = {}
    for i, num in enumerate(nums):
        complement = target - num
        if complement in seen:
            return [seen[complement], i]
        seen[num] = i


assert two_sum([2, 7, 11, 15], 9) == [0, 1]
assert two_sum([3, 2, 4], 6) == [1, 2]
assert two_sum([3, 3], 6) == [0, 1]
print("Two Sum: all tests passed")


# ============================================================
# 2. Group Anagrams
# ============================================================
# Problem: Given a list of strings, group the anagrams together.
#          Return the groups in any order.
# Example: ["eat","tea","tan","ate","nat","bat"]
#          → [["eat","tea","ate"], ["tan","nat"], ["bat"]]
# Key insight: anagrams share the same sorted characters → use sorted word as key
# Time: O(n * k log k) | Space: O(n * k)   k = max word length

def group_anagrams(strs):
    groups = defaultdict(list)
    for word in strs:
        key = "".join(sorted(word))   # "eat" → "aet", "tea" → "aet", "ate" → "aet"
        groups[key].append(word)
    return list(groups.values())


result = group_anagrams(["eat", "tea", "tan", "ate", "nat", "bat"])
assert sorted([sorted(g) for g in result]) == sorted([sorted(g) for g in [["eat","tea","ate"],["tan","nat"],["bat"]]])
assert group_anagrams([""]) == [[""]]
assert group_anagrams(["a"]) == [["a"]]
print("Group Anagrams: all tests passed")
