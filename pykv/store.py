# ============================ store.py ============================
# This file implements the MAIN IN-MEMORY KEY VALUE STORE.
# Key features:
# 1) O(1) GET/SET/DELETE using hash map
# 2) LRU eviction using doubly linked list
# 3) TTL expiration (lazy + background cleanup)
# 4) Metrics tracking (hits, misses, uptime etc.)
# ================================================================

import time        # Used for TTL and uptime
import threading   # Used for background TTL cleanup


class Node:
    """
    Doubly linked list node used for LRU cache.
    Each node stores one key-value pair.
    """
    def __init__(self, key, value, expiry=None):
        self.key = key            # Stores the key
        self.value = value        # Stores the value
        self.expiry = expiry      # Stores expiration timestamp for TTL

        self.prev = None          # Pointer to previous node
        self.next = None          # Pointer to next node


class PyKVStore:
    """
    In-memory KV store using:
    - Hash map for O(1) access
    - Doubly linked list for LRU order
    - TTL support
    """
    def __init__(self, capacity=1000):
        # Maximum allowed keys in the store
        self.capacity = capacity

        # Dict to map key -> node (fast lookup)
        self.map = {}

        # Create dummy head and tail nodes for LRU list
        self.head = Node(None, None)   # head.next = most recently used key
        self.tail = Node(None, None)   # tail.prev = least recently used key

        # Connect head and tail
        self.head.next = self.tail
        self.tail.prev = self.head

        # ---------- METRICS ----------
        self.total_ops = 0
        self.cache_hits = 0
        self.cache_misses = 0
        self.evictions = 0
        self.ttl_expirations = 0
        self.start_time = time.time()   # Used to calculate uptime

        # Background thread stop flag
        self._stop = False

        # Start TTL cleanup background thread
        self.cleanup_thread = threading.Thread(
            target=self._ttl_cleanup_loop,  # function to run
            daemon=True                     # stops when program exits
        )
        self.cleanup_thread.start()

    # ------------------- LRU INTERNAL METHODS -------------------

    def _add_to_front(self, node: Node):
        """
        Adds node right after head => Most recently used
        """
        node.next = self.head.next            # node points to old first node
        node.prev = self.head                 # node.prev becomes head
        self.head.next.prev = node            # old first node prev becomes node
        self.head.next = node                 # head.next becomes node

    def _remove_node(self, node: Node):
        """
        Removes node from doubly linked list
        """
        node.prev.next = node.next            # skip node forward
        node.next.prev = node.prev            # skip node backward

    def _move_to_front(self, node: Node):
        """
        Marks node as recently used by moving it to front
        """
        self._remove_node(node)               # remove from current position
        self._add_to_front(node)              # add again to front

    def _evict_lru(self):
        """
        Removes least recently used item when capacity exceeded
        """
        lru = self.tail.prev                  # last real node is LRU

        # If store is empty (only dummy nodes)
        if lru == self.head:
            return

        # Remove lru node from list and dict
        self._remove_node(lru)
        self.map.pop(lru.key, None)

        # Increase eviction counter
        self.evictions += 1

    # ------------------- TTL METHODS -------------------

    def _is_expired(self, node: Node) -> bool:
        """
        Checks if the key is expired (TTL over)
        """
        return node.expiry is not None and time.time() >= node.expiry

    def _ttl_cleanup_loop(self):
        """
        Background thread which deletes expired keys regularly
        """
        while not self._stop:
            time.sleep(2)                     # run cleanup every 2 seconds
            now = time.time()                 # current timestamp

            expired_keys = []

            # Scan all keys and find expired ones
            for k, node in list(self.map.items()):
                if node.expiry is not None and now >= node.expiry:
                    expired_keys.append(k)

            # Delete all expired keys
            for k in expired_keys:
                self.ttl_expirations += 1     # increase expiry counter
                self.delete(k, reason="ttl")  # delete key from store

    # ------------------- PUBLIC STORE METHODS -------------------

    def set(self, key, value, ttl=None):
        """
        SET operation:
        - Insert or update key value
        - Add TTL if given
        """
        self.total_ops += 1                   # update operation count

        # Convert TTL seconds to absolute expiry timestamp
        expiry = None
        if ttl is not None:
            expiry = time.time() + ttl

        # If key already exists: update value
        if key in self.map:
            node = self.map[key]              # get node
            node.value = value                # update value
            node.expiry = expiry              # update expiry
            self._move_to_front(node)         # mark as recently used
            return True

        # If store full: evict LRU
        if len(self.map) >= self.capacity:
            self._evict_lru()

        # Create new node
        node = Node(key, value, expiry)

        # Store in dict
        self.map[key] = node

        # Add to LRU front
        self._add_to_front(node)

        return True

    def get(self, key):
        """
        GET operation:
        - Return value if present
        - Update LRU order
        - Check TTL expiration
        """
        self.total_ops += 1

        node = self.map.get(key)

        # If key not found
        if not node:
            self.cache_misses += 1
            return None

        # If key expired
        if self._is_expired(node):
            self.ttl_expirations += 1
            self.delete(key, reason="ttl")
            self.cache_misses += 1
            return None

        # Key found => update LRU
        self._move_to_front(node)

        self.cache_hits += 1
        return node.value

    def delete(self, key, reason="user"):
        """
        DELETE operation:
        - Remove key from map + LRU list
        """
        self.total_ops += 1

        node = self.map.get(key)

        # If key not found
        if not node:
            return False

        # Remove from list and dict
        self._remove_node(node)
        self.map.pop(key, None)

        return True

    def ttl_remaining(self, key):
        """
        Returns remaining TTL seconds for the key
        """
        node = self.map.get(key)

        # If key not found or no TTL
        if not node or node.expiry is None:
            return None

        # Calculate remaining seconds
        remaining = int(node.expiry - time.time())

        # Never return negative values
        return max(0, remaining)

    def stats(self, wal_size=0):
        """
        Returns store statistics for dashboard
        """
        uptime = int(time.time() - self.start_time)

        return {
            "total_keys": len(self.map),
            "total_ops": self.total_ops,
            "cache_hits": self.cache_hits,
            "cache_misses": self.cache_misses,
            "evictions": self.evictions,
            "ttl_expirations": self.ttl_expirations,
            "uptime_seconds": uptime,
            "wal_file_size": wal_size,
        }

    def keys(self):
        """
        Returns list of all keys in store
        """
        return list(self.map.keys())

    def stop(self):
        """
        Stops TTL cleanup thread
        """
        self._stop = True
