"""分区移交记录领域服务：受理和确认消费组的分区归属移交，隔离新旧接入方权限。

背景
----
两个消息流源可以同时登记 `orders` 主题的同一个分区（例如分区 0）。消息中间件里的
`orders/0` 是同一段物理日志，但“谁在处理、处理到哪、哪些历史可以接续”必须按
`来源源 + 消费组 + Topic + 分区` 精确记账。旧源把偏移量推进到 40，不代表新源可以从
40 接着读；旧源在途的 40–45 也不能被新源用检查点确认。

本模块维护三类事实：
1. 移交记录：来源源、目标源、消费组、Topic、分区、旧方在途区间、可共享区间和
   允许接续边界。状态为 待受理 -> 已受理 -> 已确认；旧方在途未收尾时停在
   “待旧方收尾”，而不是把同一段数据交给两边。
2. 移交凭据：受理时按角色签发。凭据里固化来源、消费组、分区和允许认领的区间，
   旧方与 New 方各持有自己的凭据，不能互换使用。
3. 认领账本：所有在途提交和已确认检查点都会登记一段 offset 区间。同一
   `来源源 + 消费组 + Topic + 分区` 下的区间只能被一个角色认领一次。

数据链路位置：消息接入层的所有权边界，位于消息流源、偏移量管理和记录检查点之间。
"""
from __future__ import annotations

import re
import secrets
from datetime import datetime, timezone
from typing import Any, Iterable

from app.services.base import BaseModuleService
from app.store import store

ROLE_OLD = "旧接入方"
ROLE_NEW = "新接入方"

STATUS_PENDING = "待受理"
STATUS_ACCEPTED = "已受理"
STATUS_WAITING_OLD = "待旧方收尾"
STATUS_CONFIRMED = "已确认"
STATUS_REJECTED = "已驳回"

_OLD_ROLE_ALIASES = {
    ROLE_OLD, "旧方", "原接入方", "原接入人", "old", "old_side", "from", "old_source",
}
_NEW_ROLE_ALIASES = {
    ROLE_NEW, "新方", "新接入人", "new", "new_side", "to", "new_source",
}

_FIELD_ALIASES: dict[str, tuple[str, ...]] = {
    "来源源": (
        "来源源", "旧源", "原源", "旧接入方", "来源", "source", "old_source",
        "from_source", "old", "from",
    ),
    "目标源": (
        "目标源", "新源", "新接入方", "目标", "target", "new_source",
        "to_source", "new", "to",
    ),
    "消费组": ("消费组", "消费组名", "group", "group_id", "consumer_group", "consumerGroup"),
    "Topic/Queue": ("Topic/Queue", "Topic", "topic", "队列", "主题", "topic_name"),
    "分区/分片": ("分区/分片", "分区", "分片", "partition", "partition_id", "shard"),
    "旧方归属区间": (
        "旧方归属区间", "旧方区间", "旧方认领区间", "在途区间", "旧方在途区间",
        "old_range", "old_claim", "inflight_range", "claim_range",
    ),
    "允许接续区间": (
        "允许接续区间", "移交区间", "可接续区间", "可共享区间", "共享区间",
        "handoff_range", "shareable_range", "resume_range",
    ),
    "允许接续边界": (
        "允许接续边界", "移交边界", "接续边界", "boundary", "handoff_boundary",
        "resume_boundary", "max_offset",
    ),
    "旧方起点": ("旧方起点", "旧方区间起点", "old_start", "claim_start"),
    "旧方终点": ("旧方终点", "旧方区间终点", "old_end", "claim_end"),
    "可共享起点": ("可共享起点", "移交起点", "handoff_start", "shareable_start"),
    "可共享终点": ("可共享终点", "移交终点", "handoff_end", "shareable_end"),
}

_RANGE_SEPARATORS = re.compile(r"\s*(?:\.\.|~|～|—|–|至|到|,|:|：|-)\s*")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _pick(raw: dict[str, Any], canonical: str, default: Any = None) -> Any:
    """按中英文别名取字段，兼容前端表单和脚本调用。"""
    for name in _FIELD_ALIASES.get(canonical, (canonical,)):
        if name in raw:
            value = raw[name]
            if value is not None and str(value).strip() != "":
                return value
    return default


def normalize_role(value: str | None) -> str | None:
    if value is None:
        return None
    role = str(value).strip()
    if role in _OLD_ROLE_ALIASES:
        return ROLE_OLD
    if role in _NEW_ROLE_ALIASES:
        return ROLE_NEW
    return None


def _to_int(value: Any, field: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{field}必须是整数")
    if isinstance(value, int):
        return value
    text = str(value).strip()
    if not text:
        raise ValueError(f"{field}不能为空")
    try:
        return int(text)
    except ValueError as exc:
        raise ValueError(f"{field}必须是整数，收到 {value!r}") from exc


def parse_range(value: Any, start: Any = None, end: Any = None,
                field: str = "区间") -> tuple[int, int]:
    """把 "40-45"、[40,45]、{"start":40,"end":45} 或起止字段统一成闭区间。"""
    if value is not None and not isinstance(value, (dict, list, tuple)):
        text = str(value).strip().strip("[]()（）")
        text = text.replace("，", ",")
        parts = _RANGE_SEPARATORS.split(text, maxsplit=1)
        if len(parts) == 2:
            return _to_int(parts[0], f"{field}起点"), _to_int(parts[1], f"{field}终点")
        if text in ("", "无", "none", "None", "-"):
            raise ValueError(f"{field}不能为空")
        if start is None and end is None:
            point = _to_int(text, field)
            return point, point
    if isinstance(value, dict):
        start = value.get("start", value.get("起点", value.get("from")))
        end = value.get("end", value.get("终点", value.get("to")))
    elif isinstance(value, (list, tuple)):
        if len(value) != 2:
            raise ValueError(f"{field}必须是两个端点")
        start, end = value
    if start is None or end is None:
        raise ValueError(f"{field}必须给出起点和终点")
    first, last = _to_int(start, f"{field}起点"), _to_int(end, f"{field}终点")
    return first, last


def format_range(start: int, end: int) -> str:
    return f"{start}-{end}"


def _overlap(first: tuple[int, int], second: tuple[int, int]) -> bool:
    return first[0] <= second[1] and second[0] <= first[1]


def _credential_store() -> dict[str, dict[str, Any]]:
    return store.aux.setdefault("handoff_credentials", {})


def _claim_store() -> list[dict[str, Any]]:
    return store.aux.setdefault("handoff_claims", [])


def find_credential(token: str | None) -> dict[str, Any] | None:
    if not token:
        return None
    return _credential_store().get(str(token))


def all_claims() -> list[dict[str, Any]]:
    return _claim_store()


def find_handoff(handoff_id: int) -> dict[str, Any] | None:
    return store.find("partition_handoff", handoff_id)


def _handoff_source(handoff: dict[str, Any], role: str) -> str:
    return str(handoff.get("来源源" if role == ROLE_OLD else "目标源", ""))


def _source_key(source: str, group: str, topic: str, partition: str) -> tuple[str, str, str, str]:
    return (
        str(source).strip(),
        str(group).strip(),
        str(topic).strip(),
        str(partition).strip(),
    )


def _claim_key_from_handoff(handoff: dict[str, Any]) -> tuple[str, str, str, str]:
    return _source_key(
        str(handoff.get("来源源", "")),
        str(handoff.get("消费组", "")),
        str(handoff.get("Topic/Queue", "")),
        str(handoff.get("分区/分片", "")),
    )


def credential_context(token: str | None) -> tuple[dict[str, Any] | None, dict[str, Any] | None, str]:
    """校验凭据并返回 (凭据, 移交记录, 错误说明)。"""
    credential = find_credential(token)
    if credential is None:
        return None, None, "移交凭据不存在或已失效"
    if credential.get("state") != "有效":
        return credential, None, "移交凭据已被撤销"
    handoff = find_handoff(int(credential.get("handoff_id", 0)))
    if handoff is None:
        return credential, None, "移交记录不存在"
    if handoff.get("status") == STATUS_REJECTED:
        return credential, handoff, "移交记录已驳回，凭据不可用"
    role = str(credential.get("role", ""))
    expected_source = _handoff_source(handoff, role)
    if str(credential.get("来源源", "")) != expected_source:
        return credential, handoff, "凭据来源与移交记录不一致"
    for field in ("消费组", "Topic/Queue", "分区/分片"):
        if str(credential.get(field, "")) != str(handoff.get(field, "")):
            return credential, handoff, f"凭据的{field}与移交记录不一致"
    if int(credential.get("允许接续边界", -1)) != int(handoff.get("允许接续边界", -2)):
        return credential, handoff, "凭据的接续边界与移交记录不一致"
    return credential, handoff, ""


def _allowed_interval(credential: dict[str, Any]) -> tuple[int, int]:
    return int(credential.get("允许起点", 0)), int(credential.get("允许终点", 0))


def _owner_of_offset(handoff: dict[str, Any], offset: int) -> str:
    old_start = int(handoff.get("旧方起点", 0))
    old_end = int(handoff.get("旧方终点", -1))
    share_start = int(handoff.get("可共享起点", 0))
    boundary = int(handoff.get("允许接续边界", -1))
    if old_start <= offset <= old_end:
        return ROLE_OLD
    if share_start <= offset <= boundary:
        return ROLE_NEW
    return "未移交"


def _out_of_range_reason(handoff: dict[str, Any], role: str, start: int, end: int) -> str:
    if role == ROLE_NEW:
        old_start = int(handoff.get("旧方起点", 0))
        old_end = int(handoff.get("旧方终点", -1))
        boundary = int(handoff.get("允许接续边界", -1))
        if start <= old_end and end >= old_start:
            return "该区间是旧方未共享的在途数据，只允许原接入方处理"
        if end > boundary:
            return f"超出允许接续边界 {boundary}"
        return "该区间不在新接入方的可接续历史区间内"
    share_start = int(handoff.get("可共享起点", 0))
    boundary = int(handoff.get("允许接续边界", -1))
    if start <= boundary and end >= share_start:
        return "该区间已移交新接入方，旧接入方不得再认领"
    return "该区间不在旧接入方的归属区间内"


def check_segment(token: str | None, start: int, end: int,
                  checkpoint_id: int | None = None) -> dict[str, Any]:
    """检查某个凭据能否认领 [start, end]；供检查点和偏移量模块复用。

    `checkpoint_id` 用于区分“同一个检查点重放”和“另一个检查点想再确认同一段数据”：
    前者幂等，后者必须拒绝，避免新旧双方（或同一方多次）对同一段 offset 重复确认。
    """
    credential, handoff, error = credential_context(token)
    if error or credential is None or handoff is None:
        return {"ok": False, "available": False, "message": error or "凭据不可用"}
    if start > end:
        return {"ok": False, "available": False, "message": "区间起点不能大于终点"}
    role = str(credential.get("role"))
    allowed_start, allowed_end = _allowed_interval(credential)
    if start < allowed_start or end > allowed_end:
        return {
            "ok": False,
            "available": False,
            "message": _out_of_range_reason(handoff, role, start, end),
            "role": role,
            "source": credential.get("来源源"),
            "allowed_range": format_range(allowed_start, allowed_end),
            "owner": _owner_of_offset(handoff, start if start < allowed_start else end),
        }

    key = _source_key(
        str(credential.get("来源源", "")),
        str(credential.get("消费组", "")),
        str(credential.get("Topic/Queue", "")),
        str(credential.get("分区/分片", "")),
    )
    for claim in _claim_store():
        if claim.get("state") not in ("在途待确认", STATUS_CONFIRMED):
            continue
        claim_key = tuple(str(claim.get(field, "")) for field in ("来源源", "消费组", "Topic/Queue", "分区/分片"))
        if claim_key != key:
            continue
        claim_range = (int(claim.get("起点", 0)), int(claim.get("终点", -1)))
        if not _overlap((start, end), claim_range):
            continue
        same_claim = claim.get("credential") == token and claim_range == (start, end)
        if same_claim and checkpoint_id is not None and claim.get("checkpoint_id") == checkpoint_id:
            return {"ok": True, "available": True, "idempotent_claim": claim,
                    "credential": credential, "handoff": handoff}
        if same_claim and claim.get("checkpoint_id") is None:
            return {"ok": True, "available": True, "idempotent_claim": claim,
                    "credential": credential, "handoff": handoff}
        if same_claim and claim.get("state") == "在途待确认":
            return {"ok": True, "available": True, "inflight_claim": claim,
                    "credential": credential, "handoff": handoff}
        if same_claim and claim.get("checkpoint_id") == checkpoint_id:
            return {"ok": True, "available": True, "idempotent_claim": claim,
                    "credential": credential, "handoff": handoff}
        if same_claim:
            return {
                "ok": False,
                "available": False,
                "conflict": True,
                "message": (
                    f"区间 {format_range(start, end)} 已确认过，"
                    "不能被另一个检查点重复确认"
                ),
                "owner": claim.get("role"),
                "owner_source": claim.get("来源源"),
                "owned_range": format_range(*claim_range),
                "existing_claim": claim,
                "credential": credential,
                "handoff": handoff,
            }
        return {
            "ok": False,
            "available": False,
            "conflict": True,
            "message": (
                f"区间 {format_range(start, end)} 已由{claim.get('role', '另一方')}"
                f"以 {format_range(*claim_range)} 认领，检查点不能被两边同时确认"
            ),
            "owner": claim.get("role"),
            "owner_source": claim.get("来源源"),
            "owned_range": format_range(*claim_range),
            "existing_claim": claim,
            "credential": credential,
            "handoff": handoff,
        }
    return {"ok": True, "available": True, "credential": credential, "handoff": handoff}


def register_claim(handoff_id: int, token: str, role: str, start: int, end: int,
                   state: str, checkpoint_id: int | None = None) -> dict[str, Any]:
    """登记一段认领区间；同凭据同区间重复登记时更新原记录。"""
    for claim in _claim_store():
        if (int(claim.get("handoff_id", 0)) == handoff_id
                and claim.get("credential") == token
                and int(claim.get("起点", -1)) == start
                and int(claim.get("终点", -1)) == end):
            # 已确认的区间不会被后续“在途待确认”降级，避免重复提交导致归属回退。
            if claim.get("state") == STATUS_CONFIRMED and state != STATUS_CONFIRMED:
                return claim
            claim["state"] = state
            if checkpoint_id is not None:
                claim["checkpoint_id"] = checkpoint_id
            if state == STATUS_CONFIRMED and not claim.get("确认时间"):
                claim["确认时间"] = _now()
            return claim
    claims = _claim_store()
    claim_id = max((int(item.get("id", 0)) for item in claims), default=0) + 1
    claim = {
        "id": claim_id,
        "handoff_id": handoff_id,
        "credential": token,
        "role": role,
        "来源源": "",
        "消费组": "",
        "Topic/Queue": "",
        "分区/分片": "",
        "起点": start,
        "终点": end,
        "state": state,
        "checkpoint_id": checkpoint_id,
        "创建时间": _now(),
        "确认时间": _now() if state == STATUS_CONFIRMED else "",
    }
    credential = find_credential(token)
    if credential:
        for field in ("来源源", "消费组", "Topic/Queue", "分区/分片"):
            claim[field] = credential.get(field, "")
    claims.append(claim)
    return claim


def claim_effective_breakpoint(token: str | None) -> int:
    """凭据视角的有效断点：已确认区间终点+1，但不超过该角色的允许终点。"""
    credential, handoff, error = credential_context(token)
    if error or credential is None or handoff is None:
        return 0
    allowed_start, allowed_end = _allowed_interval(credential)
    key = tuple(str(credential.get(field, "")) for field in ("来源源", "消费组", "Topic/Queue", "分区/分片"))
    confirmed = [
        claim for claim in _claim_store()
        if claim.get("state") == STATUS_CONFIRMED
        and tuple(str(claim.get(field, "")) for field in ("来源源", "消费组", "Topic/Queue", "分区/分片")) == key
    ]
    if not confirmed:
        return allowed_start
    highest = max(int(claim.get("终点", -1)) for claim in confirmed) + 1
    return max(allowed_start, min(highest, allowed_end + 1))


def list_claims(handoff_id: int | None = None, source: str | None = None) -> list[dict[str, Any]]:
    claims = _claim_store()
    if handoff_id is not None:
        claims = [claim for claim in claims if int(claim.get("handoff_id", 0)) == int(handoff_id)]
    if source is not None:
        claims = [claim for claim in claims if str(claim.get("来源源", "")) == str(source)]
    return [dict(claim) for claim in claims]


def revoke_credentials(handoff_id: int) -> None:
    for credential in _credential_store().values():
        if int(credential.get("handoff_id", 0)) == int(handoff_id):
            credential["state"] = "已撤销"


class PartitionHandoffService(BaseModuleService):
    MODULE = "partition_handoff"
    ENTITY = "分区移交记录"
    OPERATION_TERM = "受理"
    LIST_FIELDS = [
        "来源源", "目标源", "消费组", "Topic/Queue", "分区/分片",
        "旧方归属区间", "允许接续区间", "允许接续边界",
    ]
    REQUIRED_FIELDS = ["来源源", "目标源", "消费组", "Topic/Queue", "分区/分片"]
    STATUS_ORDER = [STATUS_PENDING, STATUS_ACCEPTED, STATUS_WAITING_OLD, STATUS_CONFIRMED, STATUS_REJECTED]
    ACTION_RULES = {
        "受理移交": STATUS_ACCEPTED,
        "确认移交": STATUS_CONFIRMED,
        "驳回移交": STATUS_REJECTED,
    }
    NEGATIVE_ACTIONS = ["驳回移交"]

    # ---------- 登记 / 受理 ----------

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        entry, errors = self.create_handoff(values)
        return entry, errors

    def create_handoff(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        raw = dict(values or {})
        old_source = str(_pick(raw, "来源源", "")).strip()
        new_source = str(_pick(raw, "目标源", "")).strip()
        group = str(_pick(raw, "消费组", "")).strip()
        topic = str(_pick(raw, "Topic/Queue", "")).strip()
        partition = str(_pick(raw, "分区/分片", "0")).strip() or "0"

        missing = [
            label for label, value in (
                ("来源源", old_source), ("目标源", new_source),
                ("消费组", group), ("Topic/Queue", topic), ("分区/分片", partition),
            ) if not value
        ]
        if missing:
            return None, missing
        if old_source == new_source:
            return None, ["来源源和目标源不能是同一个消息流源"]

        # 同一来源的同一分区只能有一条移交代办：旧数据一旦按某条移交确认过，
        # 再开一条会把同一段 offset 重新交出去，认领账本也就失去意义。
        for row in store.rows(self.MODULE):
            if (
                row.get("status") != STATUS_REJECTED
                and str(row.get("来源源")) == old_source
                and str(row.get("消费组")) == group
                and str(row.get("Topic/Queue")) == topic
                and str(row.get("分区/分片")) == partition
            ):
                return None, [
                    f"该分区已有移交记录 #{row.get('id')}（{row.get('status')}），"
                    "同一来源的同一分区不能重复移交"
                ]

        try:
            old_value = _pick(raw, "旧方归属区间")
            if old_value is None:
                old_start = _to_int(_pick(raw, "旧方起点"), "旧方起点")
                old_end = _to_int(_pick(raw, "旧方终点"), "旧方终点")
            else:
                old_start, old_end = parse_range(old_value, field="旧方归属区间")
            share_value = _pick(raw, "允许接续区间")
            if share_value is None:
                share_start = _to_int(_pick(raw, "可共享起点", old_end + 1), "可共享起点")
                share_end = _to_int(_pick(raw, "可共享终点", share_start), "可共享终点")
            else:
                share_start, share_end = parse_range(share_value, field="允许接续区间")
            boundary_value = _pick(raw, "允许接续边界")
            boundary = _to_int(boundary_value, "允许接续边界") if boundary_value is not None else share_end
        except ValueError as exc:
            return None, [str(exc)]

        if old_start < 0 or old_end < old_start:
            return None, ["旧方归属区间必须是递增的非负闭区间"]
        if share_start < 0 or share_end < share_start:
            return None, ["允许接续区间必须是递增的非负闭区间"]
        if share_start <= old_end:
            return None, ["可共享区间必须位于旧方在途区间之后"]
        if share_end > boundary:
            return None, ["允许接续区间不能越过允许接续边界"]
        if boundary < share_end:
            return None, ["允许接续边界不能小于可共享区间终点"]

        rows = store.rows(self.MODULE)
        entry_id = max((int(row.get("id", 0)) for row in rows), default=0) + 1
        entry: dict[str, Any] = {
            "id": entry_id,
            "status": STATUS_PENDING,
            "pending": True,
            "abnormal": False,
            "来源源": old_source,
            "目标源": new_source,
            "消费组": group,
            "Topic/Queue": topic,
            "分区/分片": partition,
            "旧方归属区间": format_range(old_start, old_end),
            "允许接续区间": format_range(share_start, share_end),
            "允许接续边界": boundary,
            "旧方起点": old_start,
            "旧方终点": old_end,
            "可共享起点": share_start,
            "可共享终点": share_end,
            "受理时间": "",
            "确认时间": "",
            "旧方凭据": "",
            "新方凭据": "",
            "旧方在途状态": "",
            "冲突说明": "",
            "驳回原因": "",
        }
        rows.append(entry)
        return entry, []

    def accept_handoff(self, handoff_id: int) -> dict[str, Any]:
        entry = find_handoff(handoff_id)
        if entry is None:
            return {"ok": False, "message": f"分区移交记录 {handoff_id} 不存在"}
        if entry.get("status") == STATUS_REJECTED:
            return {"ok": False, "message": "已驳回的移交记录不能受理", "entry": entry}
        if entry.get("status") == STATUS_CONFIRMED:
            credentials = self.get_credentials(handoff_id)
            return {"ok": True, "message": "移交已确认，无需重复受理",
                    "entry": entry, "credentials": credentials}

        if entry.get("status") in (STATUS_PENDING, STATUS_WAITING_OLD):
            entry["status"] = STATUS_ACCEPTED
            entry["pending"] = True
            entry["受理时间"] = entry.get("受理时间") or _now()
        credentials = self._ensure_credentials(entry)
        self._ensure_offsets(entry)
        return {
            "ok": True,
            "message": (
                f"已受理 {entry.get('来源源')}→{entry.get('目标源')} "
                f"{entry.get('Topic/Queue')}/{entry.get('分区/分片')} 的移交"
            ),
            "entry": entry,
            "credentials": credentials,
            "boundary": entry.get("允许接续边界"),
        }

    def _ensure_credentials(self, entry: dict[str, Any]) -> list[dict[str, Any]]:
        existing = [
            credential for credential in _credential_store().values()
            if int(credential.get("handoff_id", 0)) == int(entry["id"])
            and credential.get("state") == "有效"
        ]
        if len(existing) >= 2:
            return existing

        old_start, old_end = int(entry["旧方起点"]), int(entry["旧方终点"])
        share_start = int(entry["可共享起点"])
        boundary = int(entry["允许接续边界"])
        definitions = [
            (ROLE_OLD, str(entry["来源源"]), 0, old_end, "旧方未共享区间及在途区间"),
            (ROLE_NEW, str(entry["目标源"]), share_start, boundary, "旧方在途区间之后的可接续历史"),
        ]
        issued: list[dict[str, Any]] = list(existing)
        for role, source, allowed_start, allowed_end, note in definitions:
            if any(item.get("role") == role for item in issued):
                continue
            token = secrets.token_urlsafe(18)
            credential = {
                "token": token,
                "handoff_id": int(entry["id"]),
                "role": role,
                "来源源": source,
                "对端源": str(entry["目标源"] if role == ROLE_OLD else entry["来源源"]),
                "消费组": str(entry["消费组"]),
                "Topic/Queue": str(entry["Topic/Queue"]),
                "分区/分片": str(entry["分区/分片"]),
                "允许起点": allowed_start,
                "允许终点": allowed_end,
                "允许区间": format_range(allowed_start, allowed_end),
                "允许接续边界": boundary,
                "旧方在途区间": str(entry["旧方归属区间"]),
                "可共享区间": str(entry["允许接续区间"]),
                "说明": note,
                "state": "有效",
                "签发时间": _now(),
                "已确认区间": [],
            }
            _credential_store()[token] = credential
            issued.append(credential)
            if role == ROLE_OLD:
                entry["旧方凭据"] = token
            else:
                entry["新方凭据"] = token
        return issued

    def _ensure_offsets(self, entry: dict[str, Any]) -> None:
        from app.services.offset_manager import OffsetManagerService

        offset_service = OffsetManagerService()
        for role in (ROLE_OLD, ROLE_NEW):
            offset_service.ensure_source_offset(
                _handoff_source(entry, role),
                str(entry.get("消费组", "")),
                str(entry.get("Topic/Queue", "")),
                str(entry.get("分区/分片", "")),
                initial=0,
            )

    # ---------- 确认 / 驳回 ----------

    def confirm_handoff(self, handoff_id: int, force: bool = False) -> dict[str, Any]:
        entry = find_handoff(handoff_id)
        if entry is None:
            return {"ok": False, "message": f"分区移交记录 {handoff_id} 不存在"}
        if entry.get("status") == STATUS_REJECTED:
            return {"ok": False, "message": "已驳回的移交记录不能确认", "entry": entry}
        if entry.get("status") == STATUS_PENDING:
            accepted = self.accept_handoff(handoff_id)
            if not accepted.get("ok"):
                return accepted
            entry = accepted["entry"]

        pending_old = [
            claim for claim in _claim_store()
            if int(claim.get("handoff_id", 0)) == int(handoff_id)
            and claim.get("role") == ROLE_OLD
            and claim.get("state") == "在途待确认"
        ]
        if pending_old and not force:
            entry["status"] = STATUS_WAITING_OLD
            entry["pending"] = True
            entry["旧方在途状态"] = "在途待确认"
            entry["冲突说明"] = "旧方在途结果尚未确认，移交暂缓"
            return {
                "ok": False,
                "conflict": True,
                "message": (
                    "确认移交与旧方提交的在途结果冲突：旧方在途区间仍归旧接入方，"
                    "需先以旧方凭据确认收尾"
                ),
                "entry": entry,
                "pending_claims": pending_old,
                "ownership": [
                    {"role": ROLE_OLD, "range": format_range(int(claim["起点"]), int(claim["终点"]))}
                    for claim in pending_old
                ],
            }

        entry["status"] = STATUS_CONFIRMED
        entry["pending"] = False
        entry["确认时间"] = entry.get("确认时间") or _now()
        entry["旧方在途状态"] = entry.get("旧方在途状态") or "已确认"
        entry["冲突说明"] = ""
        for credential in self.get_credentials(handoff_id):
            if credential.get("state") == "有效":
                credential["state"] = "有效"
        return {
            "ok": True,
            "message": "移交已确认，新旧接入方按各自凭据读取和确认",
            "entry": entry,
            "boundary": entry.get("允许接续边界"),
            "old_claim": entry.get("旧方归属区间"),
            "shareable_range": entry.get("允许接续区间"),
        }

    def reject_handoff(self, handoff_id: int, reason: str = "") -> dict[str, Any]:
        entry = find_handoff(handoff_id)
        if entry is None:
            return {"ok": False, "message": f"分区移交记录 {handoff_id} 不存在"}
        if entry.get("status") == STATUS_CONFIRMED:
            return {"ok": False, "message": "已确认的移交不能驳回", "entry": entry}
        entry["status"] = STATUS_REJECTED
        entry["pending"] = False
        entry["abnormal"] = True
        entry["驳回原因"] = reason or "受理人驳回"
        revoke_credentials(handoff_id)
        return {"ok": True, "message": "移交已驳回，相关凭据已撤销", "entry": entry}

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        normalized = str(action or "").strip()
        if normalized in ("受理移交", "受理", "accept"):
            result = self.accept_handoff(entry_id)
        elif normalized in ("确认移交", "确认", "confirm"):
            result = self.confirm_handoff(entry_id)
        elif normalized in ("驳回移交", "驳回", "reject"):
            result = self.reject_handoff(entry_id)
        else:
            return super().run_action(entry_id, action)
        entry = result.get("entry") or store.find(self.MODULE, entry_id)
        return entry, str(result.get("message", ""))

    # ---------- 凭据与试读 ----------

    @staticmethod
    def _task_key(entry: dict[str, Any], source: str) -> str:
        return (
            f"{source}@{entry.get('Topic/Queue', '')}/{entry.get('分区/分片', '')}"
            f"#{entry.get('消费组', '')}"
        )

    def get_credentials(self, handoff_id: int) -> list[dict[str, Any]]:
        return [
            dict(credential) for credential in _credential_store().values()
            if int(credential.get("handoff_id", 0)) == int(handoff_id)
        ]

    def credentials_by_role(self, handoff_id: int) -> dict[str, dict[str, Any]]:
        return {str(item.get("role")): item for item in self.get_credentials(handoff_id)}

    def trial_read(self, handoff_id: int, token: str | None,
                   start: int | None = None, end: int | None = None) -> dict[str, Any]:
        """按凭据试读分区数据，只返回该角色有权读取的 offset。"""
        entry = find_handoff(handoff_id)
        if entry is None:
            return {"ok": False, "message": f"分区移交记录 {handoff_id} 不存在"}
        credential, context, error = credential_context(token)
        if error or credential is None or context is None:
            return {"ok": False, "message": error or "凭据不可用"}
        if int(credential.get("handoff_id", 0)) != int(handoff_id):
            return {"ok": False, "message": "凭据不属于该移交记录"}
        if context.get("status") == STATUS_PENDING:
            return {"ok": False, "message": "移交尚未受理，凭据还不可用于读取"}

        allowed_start, allowed_end = _allowed_interval(credential)
        read_start = allowed_start if start is None else int(start)
        read_end = allowed_end if end is None else int(end)
        if read_start > read_end:
            return {"ok": False, "message": "试读起点不能大于终点"}

        read_records: list[dict[str, Any]] = []
        denied_ranges: list[dict[str, Any]] = []
        for offset in range(read_start, read_end + 1):
            if allowed_start <= offset <= allowed_end:
                read_records.append(self._partition_record(context, str(credential["来源源"]), offset))
            else:
                owner = _owner_of_offset(context, offset)
                denied_ranges.append({
                    "offset": offset,
                    "owner": owner,
                    "reason": _out_of_range_reason(context, str(credential["role"]), offset, offset),
                })

        effective = claim_effective_breakpoint(token)
        return {
            "ok": True,
            "message": (
                f"以{credential.get('role')}凭据试读 {len(read_records)} 条，"
                f"拒绝 {len(denied_ranges)} 条"
            ),
            "handoff_id": handoff_id,
            "role": credential.get("role"),
            "source": credential.get("来源源"),
            "group": credential.get("消费组"),
            "topic": credential.get("Topic/Queue"),
            "partition": credential.get("分区/分片"),
            "allowed_range": format_range(allowed_start, allowed_end),
            "boundary": credential.get("允许接续边界"),
            "read_offsets": [record["offset"] for record in read_records],
            "read_records": read_records,
            "denied_offsets": [item["offset"] for item in denied_ranges],
            "denied_ranges": denied_ranges,
            "unclaimable_offsets": sorted({item["offset"] for item in denied_ranges}),
            "effective_breakpoint": effective,
        }

    def trial_read_with_credential(self, token: str | None,
                                   start: int | None = None,
                                   end: int | None = None) -> dict[str, Any]:
        credential, _, error = credential_context(token)
        if error or credential is None:
            return {"ok": False, "message": error or "凭据不可用"}
        return self.trial_read(int(credential.get("handoff_id", 0)), token, start, end)

    @staticmethod
    def _partition_record(handoff: dict[str, Any], source: str, offset: int) -> dict[str, Any]:
        """模拟分区日志的一条记录；不同来源的同名分区按来源生成不同消息。"""
        topic = str(handoff.get("Topic/Queue", ""))
        partition = str(handoff.get("分区/分片", ""))
        return {
            "offset": offset,
            "source": source,
            "group": str(handoff.get("消费组", "")),
            "topic": topic,
            "partition": partition,
            "message_id": f"{source}:{topic}:{partition}:{offset}",
            "key": f"{topic}-{partition}-{offset}",
            "payload": f"{source}|{topic}|{partition}|{offset}",
        }

    # ---------- 旧方在途结果 ----------

    def submit_inflight_result(self, handoff_id: int, token: str | None,
                               start: int | None = None,
                               end: int | None = None) -> dict[str, Any]:
        """旧接入方提交在途结果；先登记为待确认，归属始终是旧方。"""
        entry = find_handoff(handoff_id)
        if entry is None:
            return {"ok": False, "message": f"分区移交记录 {handoff_id} 不存在"}
        if entry.get("status") == STATUS_PENDING:
            accepted = self.accept_handoff(handoff_id)
            if not accepted.get("ok"):
                return accepted
            entry = accepted["entry"]
        credential, context, error = credential_context(token)
        if error or credential is None or context is None:
            return {"ok": False, "message": error or "凭据不可用"}
        if int(credential.get("handoff_id", 0)) != int(handoff_id):
            return {"ok": False, "message": "凭据不属于该移交记录"}
        if credential.get("role") != ROLE_OLD:
            return {"ok": False, "message": "只有旧接入方凭据可以提交在途结果"}
        claim_start = int(entry["旧方起点"]) if start is None else int(start)
        claim_end = int(entry["旧方终点"]) if end is None else int(end)
        checkpoint_id = entry.get("旧方在途检查点")
        check = check_segment(token, claim_start, claim_end,
                              checkpoint_id=int(checkpoint_id) if checkpoint_id else None)
        if not check.get("ok"):
            return check

        if not checkpoint_id:
            checkpoint_id = self._append_pending_checkpoint(entry, credential, claim_start, claim_end)
            entry["旧方在途检查点"] = checkpoint_id
        claim = register_claim(
            handoff_id, str(token), ROLE_OLD, claim_start, claim_end,
            "在途待确认", int(checkpoint_id),
        )
        if claim.get("state") == STATUS_CONFIRMED:
            # 已收尾的旧方结果不允许因为重复提交又退回待确认。
            return {
                "ok": True,
                "message": f"旧方在途结果 {format_range(claim_start, claim_end)} 已确认，重复提交保持幂等",
                "entry": entry,
                "claim": claim,
                "checkpoint_id": claim.get("checkpoint_id"),
                "owner": ROLE_OLD,
            }
        entry["旧方在途状态"] = "在途待确认"
        entry["pending"] = True
        return {
            "ok": True,
            "message": (
                f"旧方在途结果已登记为待确认 {format_range(claim_start, claim_end)}，"
                "确认前不会转给新接入方"
            ),
            "entry": entry,
            "claim": claim,
            "checkpoint_id": checkpoint_id,
            "owner": ROLE_OLD,
            "conflict_with": "新接入方不得认领该区间",
        }

    def _append_pending_checkpoint(self, entry: dict[str, Any], credential: dict[str, Any],
                                   start: int, end: int) -> int:
        rows = store.rows("record_checkpoint")
        checkpoint_id = max((int(row.get("id", 0)) for row in rows), default=0) + 1
        rows.append({
            "id": checkpoint_id,
            "status": "待确认",
            "pending": True,
            "abnormal": False,
            "关联接入任务": self._task_key(entry, str(credential.get("来源源", ""))),
            "检查点类型": "分区移交-旧方在途",
            "检查点值": format_range(start, end),
            "创建时间": _now(),
            "确认状态": "待确认",
            "来源源": str(credential.get("来源源", "")),
            "消费组": str(entry.get("消费组", "")),
            "Topic/Queue": str(entry.get("Topic/Queue", "")),
            "分区/分片": str(entry.get("分区/分片", "")),
            "区间起点": start,
            "区间终点": end,
            "归属方": ROLE_OLD,
            "移交记录": int(entry["id"]),
            "凭据": str(credential.get("token", "")),
        })
        return checkpoint_id

    def resolve_old_inflight(self, handoff_id: int, token: str | None = None) -> dict[str, Any]:
        """旧方收尾：确认其待确认的在途区间，并只推进旧源自己的偏移量。"""
        entry = find_handoff(handoff_id)
        if entry is None:
            return {"ok": False, "message": f"分区移交记录 {handoff_id} 不存在"}
        if token:
            credential, _, error = credential_context(token)
            if error or credential is None:
                return {"ok": False, "message": error or "凭据不可用"}
            if credential.get("role") != ROLE_OLD:
                return {"ok": False, "message": "只有旧接入方凭据可以收尾在途结果"}
        pending = [
            claim for claim in _claim_store()
            if int(claim.get("handoff_id", 0)) == int(handoff_id)
            and claim.get("role") == ROLE_OLD
            and claim.get("state") == "在途待确认"
        ]
        if not pending:
            return {"ok": True, "message": "没有待收尾的旧方在途结果", "entry": entry, "claims": []}

        from app.services.offset_manager import OffsetManagerService
        offset_service = OffsetManagerService()
        resolved = []
        for claim in pending:
            claim["state"] = STATUS_CONFIRMED
            claim["确认时间"] = _now()
            checkpoint_id = claim.get("checkpoint_id")
            checkpoint = store.find("record_checkpoint", int(checkpoint_id)) if checkpoint_id else None
            if checkpoint is not None:
                checkpoint["status"] = "已确认"
                checkpoint["确认状态"] = "已确认"
                checkpoint["pending"] = False
                checkpoint["确认时间"] = claim["确认时间"]
            offset_service.advance_source_offset(
                str(claim.get("来源源", "")), str(claim.get("消费组", "")),
                str(claim.get("Topic/Queue", "")), str(claim.get("分区/分片", "")),
                int(claim.get("终点", 0)) + 1,
            )
            resolved.append(claim)
        entry["旧方在途状态"] = "已确认"
        entry["冲突说明"] = ""
        return {
            "ok": True,
            "message": "旧方在途结果已按旧方归属确认，可继续确认移交",
            "entry": entry,
            "claims": resolved,
            "effective_breakpoint": claim_effective_breakpoint(
                _credentials_for(handoff_id, ROLE_OLD)
            ),
        }

    # ---------- 归属视图 ----------

    def summary(self, handoff_id: int) -> dict[str, Any]:
        entry = find_handoff(handoff_id)
        if entry is None:
            return {"ok": False, "message": f"分区移交记录 {handoff_id} 不存在"}
        credentials = self.get_credentials(handoff_id)
        from app.services.offset_manager import OffsetManagerService
        offset_service = OffsetManagerService()
        offsets = {
            role: offset_service.find_source_offset(
                _handoff_source(entry, role), str(entry.get("消费组", "")),
                str(entry.get("Topic/Queue", "")), str(entry.get("分区/分片", "")),
            )
            for role in (ROLE_OLD, ROLE_NEW)
        }
        return {
            "ok": True,
            "entry": entry,
            "credentials": credentials,
            "claims": list_claims(handoff_id),
            "offsets": offsets,
            "old_effective_breakpoint": claim_effective_breakpoint(
                _credentials_for(handoff_id, ROLE_OLD)
            ),
            "new_effective_breakpoint": claim_effective_breakpoint(
                _credentials_for(handoff_id, ROLE_NEW)
            ),
        }

    def ownership(self, source: str | None = None) -> list[dict[str, Any]]:
        records = []
        for entry in store.rows(self.MODULE):
            if entry.get("status") == STATUS_REJECTED:
                continue
            if source and source not in (entry.get("来源源"), entry.get("目标源")):
                continue
            records.append({
                "handoff_id": entry.get("id"),
                "status": entry.get("status"),
                "source": entry.get("来源源"),
                "target": entry.get("目标源"),
                "group": entry.get("消费组"),
                "topic": entry.get("Topic/Queue"),
                "partition": entry.get("分区/分片"),
                "segments": [
                    {"range": entry.get("旧方归属区间"), "owner": entry.get("来源源")},
                    {"range": entry.get("允许接续区间"), "owner": entry.get("目标源")},
                ],
                "boundary": entry.get("允许接续边界"),
            })
        return records

    def has_active_handoff(self, source: str, topic: str | None = None,
                           partition: str | None = None) -> bool:
        for entry in store.rows(self.MODULE):
            if entry.get("status") == STATUS_REJECTED:
                continue
            if source not in (entry.get("来源源"), entry.get("目标源")):
                continue
            if topic is not None and str(entry.get("Topic/Queue")) != str(topic):
                continue
            if partition is not None and str(entry.get("分区/分片")) != str(partition):
                continue
            return True
        return False

    # ---------- 演示场景 ----------

    def _reset_demo(self, old_source: str, new_source: str, group: str,
                    topic: str, partition: str) -> None:
        """清理上一次演示留下的记录，保证演示可重复执行。"""
        doomed_ids = [
            int(row.get("id", 0)) for row in store.rows(self.MODULE)
            if row.get("来源源") in (old_source, new_source)
            and row.get("目标源") in (old_source, new_source)
            and str(row.get("Topic/Queue")) == topic
            and str(row.get("分区/分片")) == partition
        ]
        if doomed_ids:
            store._tables[self.MODULE] = [
                row for row in store.rows(self.MODULE)
                if int(row.get("id", 0)) not in doomed_ids
            ]
        for token, credential in list(_credential_store().items()):
            if int(credential.get("handoff_id", 0)) in doomed_ids:
                del _credential_store()[token]
        claims = [
            claim for claim in _claim_store()
            if int(claim.get("handoff_id", 0)) not in doomed_ids
        ]
        store.aux["handoff_claims"] = claims
        store._tables["record_checkpoint"] = [
            row for row in store.rows("record_checkpoint")
            if int(row.get("移交记录", 0) or 0) not in doomed_ids
        ]
        for role_source in (old_source, new_source):
            for row in store.rows("offset_manager"):
                if (
                    str(row.get("来源源", "")) == role_source
                    and str(row.get("消费组", "")) == group
                    and str(row.get("Topic/Queue", "")) == topic
                    and str(row.get("分区/分片", "")) == partition
                ):
                    row["当前Offset"] = "0"
                    row["消费延迟"] = "0"
                    row["status"] = "正常"
                    row["abnormal"] = False

    def demo_scenario(self) -> dict[str, Any]:
        """搭出 orders/0 的双源场景：旧源先领 40–45，其后 46–50 可共享。

        重复调用会先清掉上一次的演示移交、凭据、认领和检查点，让按钮每次
        都能从“旧方在途、移交未确认”的初始状态完整走一遍。
        """
        old_source = "orders-legacy-source"
        new_source = "orders-green-source"
        group = "orders-consumer-group"
        topic = "orders"
        partition = "0"

        self._reset_demo(old_source, new_source, group, topic, partition)

        from app.services.offset_manager import OffsetManagerService
        source_service = _service("stream_source")
        if source_service is not None:
            for name in (old_source, new_source):
                if not any(str(row.get("源名称", "")) == name for row in store.rows("stream_source")):
                    entry, _ = source_service.create_entry({
                        "源名称": name,
                        "中间件类型": "Kafka",
                        "Topic/Queue名": topic,
                        "消费组": group,
                        "偏移量策略": "按分区断点",
                    })
                    if entry is not None:
                        entry["status"] = "运行中"
                        source_service.register_consumer_group(int(entry["id"]))
        offset_service = OffsetManagerService()
        old_offset = offset_service.ensure_source_offset(
            old_source, group, topic, partition, initial=0)
        offset_service.advance_source_offset(old_source, group, topic, partition, 40)
        offset_service.ensure_source_offset(new_source, group, topic, partition, initial=0)

        entry, errors = self.create_handoff({
            "来源源": old_source,
            "目标源": new_source,
            "消费组": group,
            "Topic/Queue": topic,
            "分区/分片": partition,
            "旧方归属区间": "40-45",
            "允许接续区间": "46-50",
            "允许接续边界": 50,
        })
        if entry is None:
            return {"ok": False, "message": "演示场景创建失败：" + "、".join(errors)}
        accepted = self.accept_handoff(int(entry["id"]))
        if not accepted.get("ok"):
            return accepted
        credentials = accepted.get("credentials") or self.get_credentials(int(entry["id"]))
        tokens = {item.get("role"): item.get("token") for item in credentials}
        already_done = bool([
            claim for claim in _claim_store()
            if int(claim.get("handoff_id", 0)) == int(entry["id"])
            and claim.get("role") == ROLE_OLD
            and claim.get("state") == STATUS_CONFIRMED
        ])

        if already_done:
            inflight = {"ok": True, "message": "旧方在途结果已确认（演示场景已存在）"}
            blocked = {"ok": False, "message": "已确认，不再产生冲突"}
            resolved = {"ok": True, "message": "旧方在途结果已收尾"}
            conflict_status = "已确认"
            confirmed = self.confirm_handoff(int(entry["id"]))
        else:
            # 旧源先领到 40–45，并在收尾前把在途结果登记为待确认。
            inflight = self.submit_inflight_result(
                int(entry["id"]), tokens.get(ROLE_OLD), 40, 45)
            blocked = self.confirm_handoff(int(entry["id"]))
            conflict_status = str((blocked.get("entry") or {}).get("status", ""))
            resolved = self.resolve_old_inflight(int(entry["id"]), tokens.get(ROLE_OLD))
            confirmed = self.confirm_handoff(int(entry["id"]))

        return {
            "ok": confirmed.get("ok", False),
            "message": (
                "演示场景已就绪：旧源 40–45 在途并已按旧方归属收尾，"
                "46–50 已移交新源；可用新旧凭据分别试读和确认"
            ),
            "handoff_id": entry["id"],
            "entry": confirmed.get("entry", entry),
            "credentials": credentials,
            "tokens": tokens,
            "old_offset": old_offset.get("当前Offset"),
            "conflict_before_resolve": {
                "ok": blocked.get("ok"),
                "status": conflict_status,
                "message": blocked.get("message"),
            },
            "inflight": {
                "ok": inflight.get("ok"),
                "message": inflight.get("message"),
                "owner": inflight.get("owner"),
                "range": (inflight.get("claim") or {}).get("起点", None)
                and format_range(int(inflight["claim"]["起点"]), int(inflight["claim"]["终点"])),
                "checkpoint_id": inflight.get("checkpoint_id"),
            },
            "resolved": resolved,
            "confirmed": confirmed.get("ok"),
            "topic": topic,
            "partition": partition,
            "group": group,
        }


def _credentials_for(handoff_id: int, role: str) -> str | None:
    for credential in _credential_store().values():
        if int(credential.get("handoff_id", 0)) == int(handoff_id) and credential.get("role") == role:
            return str(credential.get("token"))
    return None


def _service(module: str):
    from app.services import service_for
    return service_for(module)
