from typing import Any, Dict, List, Union

class ToonSerializer:
    """
    A simplified serializer for the Token Oriented Object Notation (TOON) format.
    This implementation focuses on minimizing tokens while maintaining readability.
    It uses indentation for hierarchy and avoids unnecessary brackets and quotes.
    """
    
    def __init__(self, indent: str = "  "):
        self.indent = indent

    def dumps(self, data: Any) -> str:
        """
        Serializes a Python object to a TOON string.
        """
        return self._serialize(data, level=0)

    def _serialize(self, data: Any, level: int) -> str:
        if isinstance(data, dict):
            return self._serialize_dict(data, level)
        elif isinstance(data, list):
            return self._serialize_list(data, level)
        else:
            return self._serialize_primitive(data)

    def _serialize_dict(self, data: Dict, level: int) -> str:
        if not data:
            return "{}"
        
        lines = []
        indent_str = self.indent * level
        for key, value in data.items():
            key_str = str(key)
            if isinstance(value, (dict, list)) and value:
                # Nested complex structure
                serialized_val = self._serialize(value, level + 1)
                # Check if the first line of the value needs to be on a new line
                # For TOON, we generally want:
                # key:
                #   val
                lines.append(f"{indent_str}{key_str}:")
                lines.append(serialized_val)
            else:
                # Simple value or empty structure
                serialized_val = self._serialize(value, level) # No extra indent for inline
                lines.append(f"{indent_str}{key_str}: {serialized_val}")
        return "\n".join(lines)

    def _serialize_list(self, data: List, level: int) -> str:
        if not data:
            return "[]"
        
        lines = []
        indent_str = self.indent * level
        for item in data:
            if isinstance(item, (dict, list)) and item:
                 # Complex item
                lines.append(f"{indent_str}-")
                # We render the item at level+1, but since the '-' already adds indentation visual
                # we might sometimes want to adjust. Simplicity first:
                # - 
                #   key: val
                # Or for dicts, maybe:
                # - key: val
                serialized_item = self._serialize(item, level + 1)
                lines.append(serialized_item)
            else:
                # Primitive item
                lines.append(f"{indent_str}- {self._serialize_primitive(item)}")
        return "\n".join(lines)

    def _serialize_primitive(self, data: Any) -> str:
        if data is None:
            return "null"
        if isinstance(data, bool):
            return "true" if data else "false"
        # Avoid quoting unless necessary (simple logic for now)
        s = str(data)
        if any(char in s for char in ":{}\n#"):
             return f'"{s}"'
        return s

def dumps(data: Any) -> str:
    """Module level helper function."""
    serializer = ToonSerializer()
    return serializer.dumps(data)
