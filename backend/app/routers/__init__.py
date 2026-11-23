"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。

分区移交的固定动作路径必须先于通用资源路由注册，否则
`/api/partition_handoff/{entry_id}` 会把 `demo`、`summary` 当成 entry_id。
"""
from __future__ import annotations

from app.routers import handoff, resource

ROUTERS = [handoff, resource]
