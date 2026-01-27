from typing import Tuple, Optional

SUBCOMMAND_PARENTS = ["/merge"]

def parse_command(input: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Parse raw input into command and args.
    
    Returns:
        (command, args) or (None, None) if not a command
    """
    if not input or not input.startswith("/"):
        return None, None
    
    parts = input.strip().split(maxsplit=2)
    
    if not parts:
        return None, None
    
    base_command = parts[0].lower()
    
    if base_command in SUBCOMMAND_PARENTS and len(parts) >= 2:
        subcommand = parts[1].lower()
        full_command = f"{base_command} {subcommand}"
        args = parts[2] if len(parts) > 2 else ""
        return full_command, args
    
    args = " ".join(parts[1:]) if len(parts) > 1 else ""
    return base_command, args