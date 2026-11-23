"""消息流源领域服务：管理Kafka/Pulsar/RocketMQ消息流接入源，负责消费组注册、消息拉取和路径分流。

数据链路位置：消息接入层，通过中间件消费消息并分发给下游的去重、解析、校验等处理模块。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class StreamSourceService(BaseModuleService):
    MODULE = "stream_source"
    ENTITY = "消息流源"
    OPERATION_TERM = "接入"
    LIST_FIELDS = ["源名称", "中间件类型", "Topic/Queue名", "消费组", "偏移量策略"]
    REQUIRED_FIELDS = ["源名称", "中间件类型", "Topic/Queue名"]
    STATUS_ORDER = ["待验证", "运行中", "已暂停", "连接异常"]
    ACTION_RULES = {"启动消费": "运行中", "暂停消费": "已暂停"}
    NEGATIVE_ACTIONS = []

    # ---- 业务扩展：消息消费与路由 ----

    def register_consumer_group(self, entry_id: int) -> dict[str, Any]:
        """注册消费组并绑定偏移量策略。

        偏移量按“来源源 + 消费组 + Topic + 分区”登记；两个源可以有同名 Topic 的
        同一个分区，但各自的进度必须分开记。已有同源同分区的在途移交时，
        登记会提示走分区移交而不是直接复用旧进度。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "消息流源不存在"}
        group = str(entry.get("消费组", ""))
        topic = str(entry.get("Topic/Queue名", ""))
        source = str(entry.get("源名称", ""))
        if not group or not topic:
            return {"ok": False, "message": "消费组和Topic不能为空"}
        partition = str(entry.get("分区/分片", "0") or "0")

        from app.services.offset_manager import OffsetManagerService
        from app.services.partition_handoff import PartitionHandoffService

        handoff = PartitionHandoffService()
        conflict = None
        for other in store.rows(self.MODULE):
            if int(other.get("id", 0)) == int(entry.get("id", 0)):
                continue
            if (str(other.get("Topic/Queue名", "")) == topic
                    and str(other.get("消费组", "")) == group
                    and str(other.get("源名称", "")) != source):
                conflict = other
                break

        offset = OffsetManagerService().ensure_source_offset(
            source, group, topic, partition, initial=0)
        entry["分区/分片"] = partition
        message = f"消费组 {group} 已在来源 {source} 登记（分区已按来源隔离）"
        if conflict is not None:
            covered = handoff.has_active_handoff(source, topic, partition)
            message += (
                f"；来源 {conflict.get('源名称')} 也登记了 {topic}/{partition}，"
                + ("接续需按已受理的分区移交凭据读取" if covered
                   else "其偏移量不会沿用到本来源，接续前需先受理分区移交")
            )
        return {
            "ok": True,
            "message": message,
            "offset_id": offset.get("id"),
            "source": source,
            "topic": topic,
            "partition": partition,
            "conflict_source": conflict.get("源名称") if conflict is not None else None,
            "offset_isolated": conflict is not None,
        }

    def consume_messages(self, entry_id: int, batch_size: int = 100) -> list[dict[str, Any]]:
        """从消息流源拉取消息并生产到载荷存储。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None or entry.get("status") != "运行中":
            return []
        topic = entry.get("Topic/Queue名", "unknown")
        source = entry.get("源名称", "unknown")
        messages = []
        for i in range(min(batch_size, 3)):
            msg_id = f"msg-{entry_id}-{datetime.now(timezone.utc).timestamp():.0f}-{i}"
            messages.append({"id": msg_id, "topic": topic, "source": source, "payload": f"{topic}-data-{i}"})
        return messages

    def validate_message(self, message: dict[str, Any]) -> list[str]:
        """校验消息结构和必填字段。"""
        errors = []
        if not isinstance(message.get("payload"), str):
            errors.append("缺少消息载荷")
        if not isinstance(message.get("id"), str):
            errors.append("缺少消息ID")
        return errors

    def route_to_dedup(self, entry_id: int, message_id: str) -> str | None:
        """根据消息流源配置路由到去重中心。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None
        dedup_keys = store.rows("dedup_center")
        for key in dedup_keys:
            if key.get("status") == "已启用" and entry.get("Topic/Queue名", "").casefold() in key.get("关联数据源", "").casefold():
                return key.get("去重键名", "")
        return None

    def track_throughput(self, entry_id: int, consumed: int) -> None:
        """记录吞吐量到告警监控。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return
        alarm_rows = store.rows("throughput_alarm")
        now = datetime.now(timezone.utc).isoformat()
        for alarm in alarm_rows:
            if alarm.get("status") == "已生效" and entry.get("源名称", "") == alarm.get("监控数据源", ""):
                try:
                    floor = int(alarm.get("吞吐量下限", "0"))
                except ValueError:
                    continue
                if consumed < floor:
                    alarm["abnormal"] = True