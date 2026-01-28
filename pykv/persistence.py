# ============================ persistence.py ============================
# This file provides persistence using WAL (Write Ahead Log).
# WAL stores all operations:
# - SET
# - DEL
# So after crash/restart, data can be recovered.
# ======================================================================

import os   # Used for managing files and directories


class WAL:
    """
    WAL = Write Ahead Log class
    """
    def __init__(self, log_path="data/pykv.log"):
        self.log_path = log_path  # Store path of WAL file

        # Ensure folder exists (create if missing)
        os.makedirs(os.path.dirname(log_path), exist_ok=True)

        # If file doesn't exist create an empty file
        if not os.path.exists(self.log_path):
            open(self.log_path, "w").close()

    def append_set(self, key, value, ttl):
        """
        Writes SET operation to WAL file.
        Format:
            SET|key|value|ttl
        """
        with open(self.log_path, "a", encoding="utf-8") as f:
            f.write(f"SET|{key}|{value}|{ttl}\n")   # append to WAL

    def append_delete(self, key):
        """
        Writes DEL operation to WAL file.
        Format:
            DEL|key
        """
        with open(self.log_path, "a", encoding="utf-8") as f:
            f.write(f"DEL|{key}\n")                 # append to WAL

    def size(self):
        """
        Returns WAL file size in bytes.
        """
        return os.path.getsize(self.log_path)

    def recover(self, store):
        """
        Reads WAL file and rebuilds store state.
        This is called when server starts.
        """
        if not os.path.exists(self.log_path):
            return

        with open(self.log_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()

                # Ignore empty lines
                if not line:
                    continue

                parts = line.split("|")

                # SET recovery
                if parts[0] == "SET":
                    key = parts[1]
                    value = parts[2]
                    ttl = parts[3]

                    # Convert ttl back to int or None
                    ttl = None if ttl == "None" else int(ttl)

                    # Restore into store
                    store.set(key, value, ttl=ttl)

                # DELETE recovery
                elif parts[0] == "DEL":
                    key = parts[1]
                    store.delete(key)

    def compact(self, store):
        """
        Removes old WAL history by rewriting WAL using only live keys.
        """
        tmp_path = self.log_path + ".tmp"

        # Write new WAL file
        with open(tmp_path, "w", encoding="utf-8") as f:
            for key in store.keys():
                value = store.get(key)              # get current value
                ttl = store.ttl_remaining(key)      # get TTL remaining
                f.write(f"SET|{key}|{value}|{ttl}\n")

        # Replace old WAL with new compacted one
        os.replace(tmp_path, self.log_path)
