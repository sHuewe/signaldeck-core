# signaldeck_core/domain/group_factory.py
from typing import List
from ..models.group import Group  # adjust import if needed

def build_groups(groups_cfg: list[dict]) -> List[Group]:
    return [Group(g) for g in groups_cfg]