from typing import List, Dict


class PermissionError(Exception):
    pass


class Permissions:
    """
    Robotic agent-safe RBAC system.
    Supports:
    - Roles → Permissions mapping
    - Permission inheritance
    - Policy checks for each action
    """

    def __init__(self):
        # Expandable role map
        self.roles: Dict[str, List[str]] = {
            "user": ["read"],
            "developer": ["read", "write", "execute"],
            "admin": ["read", "write", "execute", "manage"],
        }

    def resolve_permissions(self, role: str) -> List[str]:
        if role not in self.roles:
            raise PermissionError(f"Unknown role: {role}")

        return self.roles[role]

    def has_permission(self, user_permissions: List[str], action: str) -> bool:
        return action in user_permissions

    def check(self, user_permissions: List[str], action: str):
        if not self.has_permission(user_permissions, action):
            raise PermissionError(f"Permission denied for action: {action}")
