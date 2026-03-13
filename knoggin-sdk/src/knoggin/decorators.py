"""Decorator utilities for the SDK."""

import inspect
from typing import Callable, Any, Dict

def tool(name: str = None, description: str = None) -> Callable:
    """Decorator to mark a function as an agent tool and generate its schema."""
    def decorator(func: Callable) -> Callable:
        func_name = name or func.__name__
        func_desc = description or inspect.getdoc(func) or "No description provided."
        
        sig = inspect.signature(func)
        properties = {}
        required = []
        
        for p_name, p in sig.parameters.items():
            # Very basic type inference
            p_type = "string"
            if p.annotation is int:
                p_type = "integer"
            elif p.annotation is float:
                p_type = "number"
            elif p.annotation is bool:
                p_type = "boolean"
            
            properties[p_name] = {
                "type": p_type,
                "description": f"Parameter {p_name}"
            }
            if p.default == inspect.Parameter.empty:
                required.append(p_name)
                
        # Generate the standard Knoggin tools schema expected by the backend
        schema = {
            "name": func_name,
            "description": func_desc,
            "parameters": {
                "type": "object",
                "properties": properties,
                "required": required
            }
        }
        
        func.__tool_schema__ = schema
        return func
    return decorator

def tool_to_schema(func: Callable) -> Dict[str, Any]:
    """Convert a plain function to a tool schema if it lacks one."""
    if hasattr(func, "__tool_schema__"):
        return func.__tool_schema__
    # Auto-wrap it
    return tool()(func).__tool_schema__
