"""偏移量管理领域服务：管理消费组的偏移量记录、延迟监控、回退重置和分区校验。

数据链路位置：消息消费层，跟踪每个消费组在每个分区上的消费进度，确保断点续采的偏移量准确。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class OffsetManagerService(BaseModuleService):
    MODULE = "offset_manager"
    ENTITY = "偏移量记录"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["来源源", "消费组", "Topic/Queue", "分区/分片", "当前Offset", "消费延迟"]
    REQUIRED_FIELDS = ["消费组", "Topic/Queue", "分区/分片"]
    STATUS_ORDER = ["正常", "消费延迟", "偏移回退"]
    ACTION_RULES = {"重置偏移": "消费延迟", "跳过消息": "偏移回退"}
    NEGATIVE_ACTIONS = []

    # ---- 业务扩展：按来源隔离的分区偏移 ----

    def find_source_offset(self, source: str, group: str, topic: str,
                           partition: str) -> dict[str, Any] | None:
        """按来源+消费组+Topic+分区精确查找偏移量记录。

        两个消息流源可以拥有同名 Topic 的同一个分区，这条四元组合是唯一键；
        只按 Topic+分区找会串到另一个来源的进度。
        """
        for row in store.rows(self.MODULE):
            if (
                str(row.get("来源源", "")) == str(source)
                and str(row.get("消费组", "")) == str(group)
                and str(row.get("Topic/Queue", "")) == str(topic)
                and str(row.get("分区/分片", "")) == str(partition)
            ):
                return row
        return None

    def ensure_source_offset(self, source: str, group: str, topic: str, partition: str,
                             initial: int = 0) -> dict[str, Any]:
        """不存在时为该来源建一条独立偏移量记录，已存在时原样返回。"""
        existing = self.find_source_offset(source, group, topic, partition)
        if existing is not None:
            return existing
        rows = store.rows(self.MODULE)
        new_id = max((int(row.get("id", 0)) for row in rows), default=0) + 1
        entry = {
            "id": new_id,
            "status": "正常",
            "pending": True,
            "abnormal": False,
            "来源源": str(source),
            "消费组": str(group),
            "Topic/Queue": str(topic),
            "分区/分片": str(partition),
            "当前Offset": str(initial),
            "消费延迟": "0",
        }
        rows.append(entry)
        return entry

    def advance_source_offset(self, source: str, group: str, topic: str, partition: str,
                              new_offset: int) -> dict[str, Any]:
        """只推进指定来源自己的偏移量，不触碰同名 Topic/分区的其他来源。"""
        entry = self.ensure_source_offset(source, group, topic, partition, initial=0)
        try:
            current = int(entry.get("当前Offset", "0"))
        except (ValueError, TypeError):
            current = 0
        if new_offset < current:
            entry["status"] = "偏移回退"
            entry["abnormal"] = True
            return {"ok": False, "message": "偏移回退被拒绝", "current": current,
                    "attempted": new_offset, "source": source}
        entry["当前Offset"] = str(new_offset)
        entry["消费延迟"] = str(max(0, new_offset - current))
        entry["status"] = "消费延迟" if new_offset - current > 1000 else "正常"
        return {"ok": True, "message": f"{source} 偏移推进至 {new_offset}",
                "source": source, "offset": new_offset, "entry": entry}

    def advance_offset(self, entry_id: int, new_offset: int) -> dict[str, Any]:
        """推进消费偏移量并计算延迟。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "偏移量记录不存在"}
        try:
            current = int(entry.get("当前Offset", "0"))
        except (ValueError, TypeError):
            current = 0
        if new_offset < current:
            entry["status"] = "偏移回退"
            entry["abnormal"] = True
            return {"ok": False, "message": "偏移回退被拒绝", "current": current, "attempted": new_offset}
        entry["当前Offset"] = str(new_offset)
        delay = max(0, new_offset - current)
        entry["消费延迟"] = str(delay)
        if delay > 1000:
            entry["status"] = "消费延迟"
        else:
            entry["status"] = "正常"
        return {"ok": True, "message": f"偏移推进至 {new_offset}", "delay": delay}

    def validate_checkpoint(self, entry_id: int) -> dict[str, Any]:
        """校验偏移量与检查点的对应关系；同名 Topic/分区的其他来源不算匹配。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "偏移量记录不存在"}
        group = str(entry.get("消费组", ""))
        topic = str(entry.get("Topic/Queue", ""))
        source = str(entry.get("来源源", ""))
        partition = str(entry.get("分区/分片", ""))
        checkpoints = store.rows("record_checkpoint")
        matched = [
            cp for cp in checkpoints
            if str(cp.get("消费组", "")) == group
            and str(cp.get("Topic/Queue", "")) == topic
            and str(cp.get("分区/分片", "")) == partition
            and str(cp.get("来源源", "")) == source
        ]
        return {
            "ok": True,
            "source": source,
            "group": group,
            "topic": topic,
            "partition": partition,
            "current_offset": entry.get("当前Offset"),
            "matched_checkpoints": len(matched),
        }

    def route_to_resume_policy(self, entry_id: int) -> str | None:
        """根据偏移量状态路由到匹配的续采策略。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None
        status = entry.get("status", "正常")
        policies = store.rows("resume_policy")
        for policy in policies:
            if policy.get("status") == "已启用":
                start_type = policy.get("续采起点类型", "")
                if status == "消费延迟" and "延迟" in start_type:
                    return policy.get("规则名称", "")
                if status == "偏移回退" and "回退" in start_type:
                    return policy.get("规则名称", "")
        return None

    def audit_lag(self, entry_id: int, lag: int) -> None:
        """将消费延迟记录写入审计明细。"""
        if lag <= 1000:
            return
        audit_rows = store.rows("stream_audit")
        aid = max((int(r.get("id", 0)) for r in audit_rows), default=0) + 1
        entry = store.find(self.MODULE, entry_id)
        source = entry.get("消费组", "unknown") if entry else "unknown"
        now = datetime.now(timezone.utc).isoformat()
        audit_rows.append({
            "id": aid, "处理时间": now, "数据源": source,
            "消息总数": "1", "成功数": "0", "失败原因分布": f"消费延迟{lag}",
            "status": "偏差超标", "pending": True, "abnormal": True,
        })

    def commit_watermark(self, entry_id: int, offset: int) -> str:
        """向水位追踪提交偏移量推进事件作为水位记录。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return ""
        watermark_rows = store.rows("watermark_tracker")
        wid = max((int(r.get("id", 0)) for r in watermark_rows), default=0) + 1
        now = datetime.now(timezone.utc).isoformat()
        watermark_rows.append({
            "id": wid, "数据源": entry.get("消费组", ""),
            "水位字段": "当前Offset", "当前水位": str(offset),
            "上次推进时间": now, "延迟分钟数": "0",
            "status": "正常水位", "pending": True, "abnormal": False,
        })
        return now