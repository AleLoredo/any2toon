from typing import Any, Dict, List, Union

class ToonSerializer:
    """
    Serializer for Token Oriented Object Notation (TOON).
    
    Format Logic:
    1. Arrays of Homogenous Objects (Tables):
       key[count]{col1,col2...}:
       val1,val2...
       val1,val2...
       
    2. Arrays of Primitives:
       key[count]:
       val1
       val2
       
    3. Standard Objects/Dicts:
       key:
         subkey: val
    """
    
    def __init__(self, indent: str = "  "):
        self.indent = indent

    def dumps(self, data: Any) -> str:
        # If the root is a list, we treat it as a root-level table with "root" or similar behavior?
        # The user example showed "orders[3]...". This implies the list was a value of a key "orders".
        # If we just have raw list data (like from CSV), we might need to handle it as a root table.
        # Let's say we return just the table part if it's a list.
        if isinstance(data, list):
             return self._serialize_list(data, level=0, key_context=None)
        return self._serialize(data, level=0)

    def _serialize(self, data: Any, level: int, key_context: str = None) -> str:
        if isinstance(data, dict):
            return self._serialize_dict(data, level)
        elif isinstance(data, list):
            return self._serialize_list(data, level, key_context)
        else:
            return self._serialize_primitive(data)

    def _serialize_dict(self, data: Dict, level: int) -> str:
        if not data:
            return "{}"
        
        lines = []
        indent_str = self.indent * level
        
        for key, value in data.items():
            key_str = str(key)
            if isinstance(value, list):
                # Handle list specifically to add the Header
                # We delegate to _serialize_list but pass the key so it can form "key[N]..."
                serialized_list = self._serialize_list(value, level, key_context=key_str)
                # The list serializer handles its own indentation logic typically, 
                # but if it returns a block, we just append it.
                # However, for TOON, the header IS the key line. 
                # So we don't append "key:" here. We assume _serialize_list returns "key[N]:..."
                # Exception: if _serialize_list returns "[]" (empty), we might want "key: []"
                if serialized_list == "[]":
                     lines.append(f"{indent_str}{key_str}: []")
                else:
                    # The serialized list already includes the header line with indentation
                    # We might need to adjust indentation if it doesn't match
                    lines.append(serialized_list)
            elif isinstance(value, dict) and value:
                lines.append(f"{indent_str}{key_str}:")
                lines.append(self._serialize(value, level + 1))
            else:
                serialized_val = self._serialize(value, level)
                lines.append(f"{indent_str}{key_str}: {serialized_val}")
        return "\n".join(lines)

    def _serialize_list(self, data: List, level: int, key_context: str = None) -> str:
        if not data:
            return "[]"
            
        indent_str = self.indent * level
        count = len(data)
        
        # Detect if this is a table (list of dicts with same keys)
        if self._is_homogenous_table(data):
            # Get columns from first item
            keys = list(data[0].keys())
            cols_str = ",".join(str(k) for k in keys)
            
            # Header
            if key_context:
                header = f"{indent_str}{key_context}[{count}]{{{cols_str}}}:"
            else:
                # Root level list or unnamed
                header = f"{indent_str}root[{count}]{{{cols_str}}}:"
                
            lines = [header]
            # Values
            # Note: We probably want one more level of indent for values?
            # The example showed:
            # orders[...]:
            #  101,Emma... (space indented?)
            # Usually strict alignment implies indentation. Let's add 1 indent level.
            val_indent = indent_str + " " # using 1 space as per user example? Or self.indent?
            # User example showed 1 space indent " 101,Emma..."
            
            for item in data:
                vals = [self._serialize_primitive(item[k]) for k in keys]
                line = ",".join(vals)
                lines.append(f"{val_indent}{line}")
                
            return "\n".join(lines)
            
        else:
            # Standard List (Heterogenous or Primitives)
            # key[count]:
            if key_context:
                header = f"{indent_str}{key_context}[{count}]:"
            else:
                header = f"{indent_str}root[{count}]:"
            
            lines = [header]
            val_indent = indent_str + " "
            
            for item in data:
                if isinstance(item, (dict, list)):
                     # Fallback to multiline
                     # This gets tricky in compact format. simpler to just dump?
                     # mixing modes is rare in data files.
                     # treating as generic item
                     pass
                serialized_item = self._serialize(item, level + 1)
                lines.append(f"{val_indent}{serialized_item}")
                
            return "\n".join(lines)

    def _is_homogenous_table(self, data: List) -> bool:
        """Check if list contains dicts with identical keys."""
        if not data: return False
        if not isinstance(data[0], dict): return False
        
        first_keys = set(data[0].keys())
        # Check first few items optimization? Or all? 
        # For correctness we should check all, but for speed maybe sample?
        # Let's check all for now, correctness first.
        for item in data[1:]:
            if not isinstance(item, dict): return False
            if set(item.keys()) != first_keys: return False
        return True

    def _serialize_primitive(self, data: Any) -> str:
        if data is None:
            return "null"
        if isinstance(data, bool):
            return "true" if data else "false"
        s = str(data)
        # Quote if contains delimiter
        if "," in s or "\n" in s or ":" in s:
             return f'"{s}"'
        return s

def dumps(data: Any) -> str:
    """Module level helper function."""
    serializer = ToonSerializer()
    return serializer.dumps(data)
