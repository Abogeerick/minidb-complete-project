"""
B-Tree Index Implementation

A B-tree is a self-balancing tree data structure that maintains sorted data
and allows searches, insertions, and deletions in O(log n) time.

This implementation supports:
- Configurable branching factor (order)
- Range queries
- Duplicate keys (for non-unique indexes)
- Disk persistence
"""

import os
import json
from typing import Any, List, Optional, Tuple, Dict, Iterator
from dataclasses import dataclass, field
from threading import Lock


@dataclass
class BTreeNode:
    """
    A node in the B-tree.
    
    For a B-tree of order t:
    - Each node has at most 2t-1 keys
    - Each node (except root) has at least t-1 keys
    - Each internal node has at most 2t children
    - A leaf node has no children
    """
    keys: List[Any] = field(default_factory=list)  # Key values
    values: List[List[int]] = field(default_factory=list)  # Lists of row IDs for each key
    children: List[int] = field(default_factory=list)  # Node IDs of children
    is_leaf: bool = True
    node_id: int = 0
    
    def to_dict(self) -> dict:
        return {
            'keys': self.keys,
            'values': self.values,
            'children': self.children,
            'is_leaf': self.is_leaf,
            'node_id': self.node_id,
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'BTreeNode':
        return cls(
            keys=data['keys'],
            values=data['values'],
            children=data['children'],
            is_leaf=data['is_leaf'],
            node_id=data['node_id'],
        )


class BTreeIndex:
    """
    B-Tree index implementation.
    
    This index maps column values to row IDs for fast lookups.
    Supports both unique and non-unique indexes.
    """
    
    def __init__(self, name: str, table_name: str, column_name: str, 
                 data_dir: str, order: int = 50, unique: bool = False):
        self.name = name
        self.table_name = table_name
        self.column_name = column_name
        self.data_dir = data_dir
        self.order = order  # Minimum degree (t)
        self.unique = unique
        
        self.nodes: Dict[int, BTreeNode] = {}
        self.root_id: int = 0
        self.next_node_id: int = 1
        self._lock = Lock()
        
        self.index_file = os.path.join(data_dir, f"_idx_{table_name}_{column_name}.json")
        self._load()
    
    def _load(self) -> None:
        """Load index from disk"""
        if os.path.exists(self.index_file):
            with open(self.index_file, 'r') as f:
                data = json.load(f)
                self.root_id = data.get('root_id', 0)
                self.next_node_id = data.get('next_node_id', 1)
                self.nodes = {
                    int(k): BTreeNode.from_dict(v) 
                    for k, v in data.get('nodes', {}).items()
                }
        else:
            # Initialize empty tree with root node
            root = BTreeNode(node_id=0)
            self.nodes[0] = root
            self.root_id = 0
    
    def _save(self) -> None:
        """Persist index to disk"""
        data = {
            'root_id': self.root_id,
            'next_node_id': self.next_node_id,
            'nodes': {k: v.to_dict() for k, v in self.nodes.items()},
        }
        with open(self.index_file, 'w') as f:
            json.dump(data, f, indent=2, default=str)
    
    def _allocate_node(self, is_leaf: bool = True) -> BTreeNode:
        """Allocate a new node"""
        node = BTreeNode(node_id=self.next_node_id, is_leaf=is_leaf)
        self.nodes[self.next_node_id] = node
        self.next_node_id += 1
        return node
    
    def _compare_keys(self, k1: Any, k2: Any) -> int:
        """Compare two keys, returns -1, 0, or 1"""
        if k1 is None and k2 is None:
            return 0
        if k1 is None:
            return -1
        if k2 is None:
            return 1
        
        # Convert to comparable types
        try:
            if k1 < k2:
                return -1
            elif k1 > k2:
                return 1
            return 0
        except TypeError:
            # Fall back to string comparison
            return -1 if str(k1) < str(k2) else (1 if str(k1) > str(k2) else 0)
    
    def search(self, key: Any) -> List[int]:
        """Search for all row IDs with the given key"""
        with self._lock:
            return self._search(self.root_id, key)
    
    def _search(self, node_id: int, key: Any) -> List[int]:
        """Recursive search in subtree"""
        node = self.nodes[node_id]
        
        # Find the first key greater than or equal to key
        i = 0
        while i < len(node.keys) and self._compare_keys(key, node.keys[i]) > 0:
            i += 1
        
        # If found, return the row IDs
        if i < len(node.keys) and self._compare_keys(key, node.keys[i]) == 0:
            return node.values[i].copy()
        
        # If leaf, key not found
        if node.is_leaf:
            return []
        
        # Recurse to appropriate child
        return self._search(node.children[i], key)
    
    def insert(self, key: Any, row_id: int) -> None:
        """Insert a key-rowid pair into the index"""
        with self._lock:
            root = self.nodes[self.root_id]
            
            # Check if root is full
            if len(root.keys) == 2 * self.order - 1:
                # Create new root
                new_root = self._allocate_node(is_leaf=False)
                new_root.children.append(self.root_id)
                self.root_id = new_root.node_id
                
                # Split old root
                self._split_child(new_root, 0)
                
                # Insert into non-full tree
                self._insert_non_full(new_root, key, row_id)
            else:
                self._insert_non_full(root, key, row_id)
            
            self._save()
    
    def _split_child(self, parent: BTreeNode, child_index: int) -> None:
        """Split a full child node"""
        t = self.order
        child_id = parent.children[child_index]
        child = self.nodes[child_id]
        
        # Create new node that will store (t-1) keys of child
        new_node = self._allocate_node(is_leaf=child.is_leaf)
        
        # Copy the last (t-1) keys and values to new node
        new_node.keys = child.keys[t:]
        new_node.values = child.values[t:]
        child.keys = child.keys[:t]
        child.values = child.values[:t]
        
        # If not leaf, copy children too
        if not child.is_leaf:
            new_node.children = child.children[t:]
            child.children = child.children[:t]
        
        # Insert new child into parent
        parent.children.insert(child_index + 1, new_node.node_id)
        
        # Move median key up to parent
        median_key = child.keys.pop()
        median_values = child.values.pop()
        parent.keys.insert(child_index, median_key)
        parent.values.insert(child_index, median_values)
    
    def _insert_non_full(self, node: BTreeNode, key: Any, row_id: int) -> None:
        """Insert into a non-full node"""
        i = len(node.keys) - 1
        
        if node.is_leaf:
            # Find position for new key or existing key
            while i >= 0 and self._compare_keys(key, node.keys[i]) < 0:
                i -= 1
            
            # Check if key already exists
            if i >= 0 and self._compare_keys(key, node.keys[i]) == 0:
                if self.unique and row_id not in node.values[i]:
                    raise ValueError(f"Duplicate key '{key}' in unique index")
                if row_id not in node.values[i]:
                    node.values[i].append(row_id)
            else:
                # Insert new key
                node.keys.insert(i + 1, key)
                node.values.insert(i + 1, [row_id])
        else:
            # Find child to recurse to
            while i >= 0 and self._compare_keys(key, node.keys[i]) < 0:
                i -= 1
            
            # Check if key exists at this level
            if i >= 0 and self._compare_keys(key, node.keys[i]) == 0:
                if self.unique and row_id not in node.values[i]:
                    raise ValueError(f"Duplicate key '{key}' in unique index")
                if row_id not in node.values[i]:
                    node.values[i].append(row_id)
                return
            
            i += 1
            child = self.nodes[node.children[i]]
            
            # If child is full, split it
            if len(child.keys) == 2 * self.order - 1:
                self._split_child(node, i)
                
                # After split, determine which child to descend to
                if self._compare_keys(key, node.keys[i]) > 0:
                    i += 1
                elif self._compare_keys(key, node.keys[i]) == 0:
                    # Key was moved up, add row_id here
                    if self.unique and row_id not in node.values[i]:
                        raise ValueError(f"Duplicate key '{key}' in unique index")
                    if row_id not in node.values[i]:
                        node.values[i].append(row_id)
                    return
            
            self._insert_non_full(self.nodes[node.children[i]], key, row_id)
    
    def delete(self, key: Any, row_id: int) -> bool:
        """Remove a key-rowid pair from the index"""
        with self._lock:
            result = self._delete(self.root_id, key, row_id)
            self._save()
            return result
    
    def _delete(self, node_id: int, key: Any, row_id: int) -> bool:
        """Remove row_id from the list of values for key"""
        node = self.nodes[node_id]
        
        # Find the key
        i = 0
        while i < len(node.keys) and self._compare_keys(key, node.keys[i]) > 0:
            i += 1
        
        if i < len(node.keys) and self._compare_keys(key, node.keys[i]) == 0:
            # Found the key
            if row_id in node.values[i]:
                node.values[i].remove(row_id)
                # If no more row_ids, we could remove the key entirely
                # but for simplicity we leave it (with empty list)
                return True
            return False
        
        if node.is_leaf:
            return False
        
        # Recurse to child
        return self._delete(node.children[i], key, row_id)
    
    def range_search(self, min_key: Any = None, max_key: Any = None, 
                     include_min: bool = True, include_max: bool = True) -> List[Tuple[Any, int]]:
        """Search for all entries in a key range"""
        with self._lock:
            results = []
            self._range_search(self.root_id, min_key, max_key, 
                              include_min, include_max, results)
            return results
    
    def _range_search(self, node_id: int, min_key: Any, max_key: Any,
                      include_min: bool, include_max: bool, 
                      results: List[Tuple[Any, int]]) -> None:
        """Recursive range search"""
        node = self.nodes[node_id]
        
        for i, key in enumerate(node.keys):
            # Check if we should look in left child
            if not node.is_leaf and i < len(node.children):
                if min_key is None or self._compare_keys(key, min_key) >= 0:
                    self._range_search(node.children[i], min_key, max_key,
                                      include_min, include_max, results)
            
            # Check if key is in range
            in_range = True
            if min_key is not None:
                cmp = self._compare_keys(key, min_key)
                if include_min:
                    in_range = cmp >= 0
                else:
                    in_range = cmp > 0
            
            if in_range and max_key is not None:
                cmp = self._compare_keys(key, max_key)
                if include_max:
                    in_range = cmp <= 0
                else:
                    in_range = cmp < 0
            
            if in_range:
                for row_id in node.values[i]:
                    results.append((key, row_id))
            
            # If key > max_key, no need to continue
            if max_key is not None:
                if include_max and self._compare_keys(key, max_key) > 0:
                    return
                if not include_max and self._compare_keys(key, max_key) >= 0:
                    return
        
        # Check rightmost child
        if not node.is_leaf and len(node.children) > len(node.keys):
            if max_key is None or (len(node.keys) > 0 and 
                                   self._compare_keys(node.keys[-1], max_key) < 0):
                self._range_search(node.children[-1], min_key, max_key,
                                  include_min, include_max, results)
    
    def drop(self) -> None:
        """Delete index file"""
        if os.path.exists(self.index_file):
            os.remove(self.index_file)


class IndexManager:
    """Manages all indexes for a database"""
    
    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        self.indexes: Dict[str, Dict[str, BTreeIndex]] = {}  # table -> column -> index
        self._load_indexes()
    
    def _load_indexes(self) -> None:
        """Load index metadata"""
        index_meta_file = os.path.join(self.data_dir, '_indexes.json')
        if os.path.exists(index_meta_file):
            with open(index_meta_file, 'r') as f:
                meta = json.load(f)
                for table_name, columns in meta.items():
                    self.indexes[table_name] = {}
                    for col_name, idx_info in columns.items():
                        self.indexes[table_name][col_name] = BTreeIndex(
                            name=idx_info['name'],
                            table_name=table_name,
                            column_name=col_name,
                            data_dir=self.data_dir,
                            unique=idx_info.get('unique', False)
                        )
    
    def _save_index_meta(self) -> None:
        """Save index metadata"""
        index_meta_file = os.path.join(self.data_dir, '_indexes.json')
        meta = {}
        for table_name, columns in self.indexes.items():
            meta[table_name] = {}
            for col_name, idx in columns.items():
                meta[table_name][col_name] = {
                    'name': idx.name,
                    'unique': idx.unique,
                }
        with open(index_meta_file, 'w') as f:
            json.dump(meta, f, indent=2)
    
    def create_index(self, name: str, table_name: str, column_name: str, 
                     unique: bool = False) -> BTreeIndex:
        """Create a new index"""
        table_name = table_name.lower()
        column_name = column_name.lower()
        
        if table_name not in self.indexes:
            self.indexes[table_name] = {}
        
        if column_name in self.indexes[table_name]:
            raise ValueError(f"Index already exists on {table_name}.{column_name}")
        
        idx = BTreeIndex(
            name=name,
            table_name=table_name,
            column_name=column_name,
            data_dir=self.data_dir,
            unique=unique
        )
        self.indexes[table_name][column_name] = idx
        self._save_index_meta()
        return idx
    
    def drop_index(self, table_name: str, column_name: str) -> None:
        """Drop an index"""
        table_name = table_name.lower()
        column_name = column_name.lower()
        
        if table_name in self.indexes and column_name in self.indexes[table_name]:
            self.indexes[table_name][column_name].drop()
            del self.indexes[table_name][column_name]
            self._save_index_meta()
    
    def get_index(self, table_name: str, column_name: str) -> Optional[BTreeIndex]:
        """Get an index if it exists"""
        table_name = table_name.lower()
        column_name = column_name.lower()
        
        if table_name in self.indexes:
            return self.indexes[table_name].get(column_name)
        return None
    
    def get_table_indexes(self, table_name: str) -> Dict[str, BTreeIndex]:
        """Get all indexes for a table"""
        return self.indexes.get(table_name.lower(), {})
    
    def drop_table_indexes(self, table_name: str) -> None:
        """Drop all indexes for a table"""
        table_name = table_name.lower()
        if table_name in self.indexes:
            for idx in self.indexes[table_name].values():
                idx.drop()
            del self.indexes[table_name]
            self._save_index_meta()
