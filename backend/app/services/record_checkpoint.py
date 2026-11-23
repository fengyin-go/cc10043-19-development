"""记录检查点领域服务：管理接入任务的断点位置，支持恢复和回溯，提供检查点的确认和过期管理。

数据链路位置：断点管理层，为接入任务提供可靠的断点记录和恢复依据，是续采策略的数据基础。

分区移交扩展：检查点可以绑定移交凭据。确认时必须由凭据对应的角色发起，
检查点覆盖的 offset 区间必须落在该凭据的允许区间内，且不与另一方的认领区间重叠。
这样旧方在途结果和新方的首次读取不会同时确认同一段数据。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


def _parse_range(value: Any) -> tuple[int, int] | None:
    """从 "40-45" 或单值 "40" 解析闭区间。"""
    text = str(value or "").strip()
    if not text:
        return None
    for separator in ("-", "~", "～", "—", "–"):
        if separator in text:
            left, _, right = text.partition(separator)
            try:
                return int(left.strip()), int(right.strip())
            except ValueError:
                return None
    try:
        point = int(text)
    except ValueError:
        return None
    return point, point


class RecordCheckpointService(BaseModuleService):
    MODULE = "record_checkpoint"
    ENTITY = "检查点"
    OPERATION_TERM = "接入"
    LIST_FIELDS = ["关联接入任务", "检查点类型", "检查点值", "创建时间", "确认状态"]
    REQUIRED_FIELDS = ["关联接入任务", "检查点类型", "检查点值"]
    STATUS_ORDER = ["待确认", "已确认", "已过期"]
    ACTION_RULES = {"确认检查点": "已确认", "废弃检查点": "已过期"}
    NEGATIVE_ACTIONS = []

    # ---- 业务扩展：断点记录与恢复 ----

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        """登记检查点；允许同时带上来源、消费组、分区和移交区间。

        没有这些字段的旧式检查点仍按原逻辑登记；带了凭据/移交记录的检查点
        只能走 `confirm_with_credential`，不能用通用动作直接确认。
        """
        checkpoint, missing = super().create_entry(values)
        if checkpoint is None:
            return None, missing
        optional_fields = (
            "来源源", "消费组", "Topic/Queue", "分区/分片", "区间起点", "区间终点",
            "移交记录", "凭据", "归属方",
        )
        for field in optional_fields:
            if values.get(field) not in (None, ""):
                checkpoint[field] = values[field]
        parsed = _parse_range(checkpoint.get("检查点值"))
        if parsed is not None:
            checkpoint.setdefault("区间起点", parsed[0])
            checkpoint.setdefault("区间终点", parsed[1])
        return checkpoint, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        """通用动作不能绕过移交凭据确认已绑定的检查点。

        返回 None 让路由层给出 ok=false，而不是“记录存在就算成功”。
        """
        entry = store.find(self.MODULE, entry_id)
        if (
            entry is not None
            and action == "确认检查点"
            and (entry.get("移交记录") not in (None, "") or entry.get("凭据"))
        ):
            return None, "该检查点已绑定分区移交，必须凭对应接入方凭据确认"
        return super().run_action(entry_id, action)

    def create_checkpoint(self, task_name: str, point_type: str, point_value: str) -> dict[str, Any]:
        """创建新的检查点记录。"""
        rows = store.rows(self.MODULE)
        cp_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
        now = datetime.now(timezone.utc).isoformat()
        checkpoint = {
            "id": cp_id, "关联接入任务": task_name, "检查点类型": point_type,
            "检查点值": point_value, "创建时间": now, "确认状态": "待确认",
            "status": "待确认", "pending": True, "abnormal": False,
        }
        rows.append(checkpoint)
        return {"ok": True, "message": "检查点已创建", "checkpoint": checkpoint}

    def get_latest_checkpoint(self, task_name: str) -> dict[str, Any] | None:
        """获取指定接入任务的最新已确认检查点。"""
        rows = store.rows(self.MODULE)
        matched = [r for r in rows if task_name in str(r.get("关联接入任务", ""))
                   and r.get("status") == "已确认"]
        if not matched:
            return None
        return max(matched, key=lambda r: str(r.get("创建时间", "")))

    def validate_checkpoint_chain(self, entry_id: int) -> dict[str, Any]:
        """校验检查点链的完整性：是否存在断链或顺序异常。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "检查点不存在"}
        task = entry.get("关联接入任务", "")
        rows = store.rows(self.MODULE)
        chain = sorted(
            [r for r in rows if task in str(r.get("关联接入任务", ""))],
            key=lambda r: str(r.get("创建时间", "")))
        gaps = []
        for i in range(1, len(chain)):
            prev_val = chain[i - 1].get("检查点值", "")
            curr_val = chain[i].get("检查点值", "")
            if prev_val >= curr_val:
                gaps.append(f"检查点值非递增: {prev_val} → {curr_val}")
        return {
            "ok": len(gaps) == 0,
            "task": task,
            "chain_length": len(chain),
            "gaps": gaps,
        }

    def route_to_resume(self, entry_id: int) -> dict[str, Any] | None:
        """检查点路由到续采策略，返回匹配的续采规则。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None
        point_type = entry.get("检查点类型", "")
        policies = store.rows("resume_policy")
        for policy in policies:
            if policy.get("status") == "已启用" and point_type in policy.get("续采起点类型", ""):
                return {"policy": policy, "checkpoint": entry}
        return None

    def audit_expired_checkpoints(self) -> list[int]:
        """审计过期检查点并批量标记。"""
        rows = store.rows(self.MODULE)
        expired_ids = []
        now = datetime.now(timezone.utc)
        for row in rows:
            if row.get("status") != "待确认":
                continue
            created_str = str(row.get("创建时间", ""))
            try:
                created = datetime.fromisoformat(created_str)
                if (now - created).days > 30:
                    row["status"] = "已过期"
                    expired_ids.append(int(row.get("id", 0)))
            except ValueError:
                continue
        if expired_ids:
            audit_rows = store.rows("stream_audit")
            aid = max((int(r.get("id", 0)) for r in audit_rows), default=0) + 1
            audit_rows.append({
                "id": aid, "处理时间": now.isoformat(), "数据源": "record_checkpoint",
                "消息总数": str(len(expired_ids)), "成功数": str(len(expired_ids)),
                "失败原因分布": "", "status": "正常", "pending": True, "abnormal": False,
            })
        return expired_ids

    # ---- 业务扩展：分区移交下的检查点确认 ----

    def available_history(self, token: str | None) -> dict[str, Any]:
        """凭移交凭据给出该接入方可以采用的偏移区间和有效断点。

        - 旧接入方：只看自己尚未共享、仍在途的区间，移交后的区间不可再采用。
        - 新接入方：只看到允许接续起点到边界的历史，旧方在途区间永远不在结果里。
        """
        from app.services import partition_handoff

        credential, handoff, error = partition_handoff.credential_context(token)
        if error or credential is None or handoff is None:
            return {"ok": False, "message": error or "凭据不可用"}
        if handoff.get("status") == partition_handoff.STATUS_PENDING:
            return {"ok": False, "message": "移交尚未受理，暂不能取得历史区间"}

        role = str(credential.get("role", ""))
        start, end = int(credential.get("允许起点", 0)), int(credential.get("允许终点", 0))
        effective = partition_handoff.claim_effective_breakpoint(token)
        return {
            "ok": True,
            "handoff_id": credential.get("handoff_id"),
            "role": role,
            "source": credential.get("来源源"),
            "group": credential.get("消费组"),
            "topic": credential.get("Topic/Queue"),
            "partition": credential.get("分区/分片"),
            "available_range": partition_handoff.format_range(start, end),
            "available_start": start,
            "available_end": end,
            "boundary": credential.get("允许接续边界"),
            "effective_breakpoint": effective,
            "old_inflight_range": handoff.get("旧方归属区间"),
            "shareable_range": handoff.get("允许接续区间"),
            "already_confirmed": [
                claim for claim in partition_handoff.list_claims(int(handoff["id"]))
                if claim.get("state") == partition_handoff.STATUS_CONFIRMED
            ],
        }

    def confirm_with_credential(self, entry_id: int, token: str | None,
                                start: int | None = None,
                                end: int | None = None) -> dict[str, Any]:
        """以移交凭据确认检查点；同一段数据不能让新旧双方各确认一次。"""
        from app.services import partition_handoff

        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "检查点不存在"}
        credential, handoff, error = partition_handoff.credential_context(token)
        if error or credential is None or handoff is None:
            return {"ok": False, "message": error or "凭据不可用"}
        if entry.get("status") == "已确认":
            return {"ok": False, "message": "检查点已确认，不能重复确认"}

        entry_handoff = entry.get("移交记录")
        if entry_handoff not in (None, "") and int(entry_handoff) != int(handoff["id"]):
            return {"ok": False, "message": "检查点关联的是另一条移交记录"}
        entry_source = str(entry.get("来源源", ""))
        if entry_source and entry_source != str(credential.get("来源源", "")):
            return {
                "ok": False,
                "message": (
                    f"检查点属于来源 {entry_source}，不能由 {credential.get('来源源')} 的凭据确认"
                ),
            }
        for field in ("消费组", "Topic/Queue", "分区/分片"):
            entry_value = str(entry.get(field, ""))
            if entry_value and entry_value != str(credential.get(field, "")):
                return {"ok": False, "message": f"检查点的{field}与凭据不一致"}

        if start is None or end is None:
            parsed = _parse_range(entry.get("检查点值"))
            if parsed is None:
                return {"ok": False, "message": "检查点值不是可识别的 offset 区间"}
            start, end = parsed
        start, end = int(start), int(end)

        check = partition_handoff.check_segment(token, start, end, checkpoint_id=entry_id)
        if not check.get("ok"):
            return {
                "ok": False,
                "message": check.get("message", "该区间不能由当前凭据确认"),
                "conflict": check.get("conflict", False),
                "owner": check.get("owner"),
                "owner_source": check.get("owner_source"),
                "owned_range": check.get("owned_range"),
            }

        credential["已确认区间"].append(partition_handoff.format_range(start, end))
        claim = partition_handoff.register_claim(
            int(handoff["id"]), str(token), str(credential.get("role")),
            start, end, partition_handoff.STATUS_CONFIRMED, entry_id,
        )
        entry["status"] = "已确认"
        entry["确认状态"] = "已确认"
        entry["pending"] = False
        entry["确认时间"] = claim.get("确认时间") or datetime.now(timezone.utc).isoformat()
        entry["归属方"] = credential.get("role")
        entry["确认凭据"] = str(token)
        entry["区间起点"] = start
        entry["区间终点"] = end

        # 只有检查点覆盖到该来源的接续边界时，才推进它自己的偏移量。
        offset_result = None
        if end >= int(credential.get("允许终点", end)):
            from app.services.offset_manager import OffsetManagerService
            offset_result = OffsetManagerService().advance_source_offset(
                str(credential.get("来源源", "")),
                str(credential.get("消费组", "")),
                str(credential.get("Topic/Queue", "")),
                str(credential.get("分区/分片", "")),
                end + 1,
            )

        return {
            "ok": True,
            "message": (
                f"检查点已由{credential.get('role')}确认，区间 "
                f"{partition_handoff.format_range(start, end)}"
            ),
            "entry": entry,
            "claim": claim,
            "effective_breakpoint": partition_handoff.claim_effective_breakpoint(token),
            "offset": offset_result,
        }

    def trial_read(self, token: str | None, start: int | None = None,
                   end: int | None = None) -> dict[str, Any]:
        """凭据试读检查点视角的分区数据，返回实际可读区间和有效断点。"""
        from app.services import partition_handoff

        service = partition_handoff.PartitionHandoffService()
        return service.trial_read_with_credential(token, start, end)

    def commit_offset(self, entry_id: int) -> str | None:
        """将已确认检查点提交到偏移量管理；按来源+消费组+分区各记一条。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None or entry.get("status") != "已确认":
            return None
        from app.services.offset_manager import OffsetManagerService

        source = str(entry.get("来源源", "")) or str(entry.get("关联接入任务", ""))
        group = str(entry.get("消费组", "")) or str(entry.get("关联接入任务", ""))
        topic = str(entry.get("Topic/Queue", "")) or str(entry.get("关联接入任务", ""))
        partition = str(entry.get("分区/分片", "")) or "0"
        parsed = _parse_range(entry.get("检查点值")) or (0, 0)
        result = OffsetManagerService().advance_source_offset(
            source, group, topic, partition, int(parsed[1]) + 1,
        )
        return datetime.now(timezone.utc).isoformat() if result.get("ok") else None
