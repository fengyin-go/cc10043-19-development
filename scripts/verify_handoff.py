#!/usr/bin/env python3
"""分区移交场景的端到端校验。

用法：先起后端，再运行
    backend/.venv/bin/python -I scripts/verify_handoff.py --base http://127.0.0.1:8017

校验目标：
1. 两个源可以有同名 Topic 的同一个分区，但偏移量各记各的；
2. 旧源先领 40–45，收尾前确认移交会被拦在“待旧方收尾”；
3. 新旧凭据分别只能读到自己的区间，无法认领的区间有明确归属；
4. 检查点不能被两边同时确认同一段数据。
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.request

FAILURES: list[str] = []


def call(base: str, method: str, path: str, body: dict | None = None) -> dict:
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(
        base + path, data=data, method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request) as response:
            return json.loads(response.read())
    except urllib.error.HTTPError as error:
        return {"ok": False, "http_status": error.code, "message": error.read().decode()}


def check(label: str, condition: bool, detail: object = "") -> None:
    status = "PASS" if condition else "FAIL"
    print(f"[{status}] {label}" + (f" :: {detail}" if detail != "" else ""))
    if not condition:
        FAILURES.append(label)


def find_offset(base: str, source: str, group: str, topic: str, partition: str) -> dict | None:
    rows = call(base, "GET", "/api/offset_manager?size=200").get("items", [])
    return next((
        row for row in rows
        if str(row.get("来源源")) == source
        and str(row.get("消费组")) == group
        and str(row.get("Topic/Queue")) == topic
        and str(row.get("分区/分片")) == partition
    ), None)


def new_checkpoint(base: str, values: dict) -> int | None:
    result = call(base, "POST", "/api/record_checkpoint", {"values": values})
    entry = result.get("entry") or {}
    return entry.get("id")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", default="http://127.0.0.1:8017")
    base = parser.parse_args().base

    health = call(base, "GET", "/api/health")
    check("健康检查", health.get("ok") is True, health.get("modules"))

    # --- 第 1 段：演示场景覆盖“受理+旧方在途收尾+确认移交” ---
    demo = call(base, "POST", "/api/partition_handoff/demo", {})
    check("演示场景搭建成", demo.get("ok") is True, demo.get("message"))
    demo_id = demo.get("handoff_id")
    demo_tokens = demo.get("tokens", {})
    blocked = demo.get("conflict_before_resolve", {})
    check(
        "旧方在途未收尾时确认移交被拦下",
        blocked.get("ok") is False and blocked.get("status") == "待旧方收尾",
        blocked.get("status"),
    )
    check("旧方在途检查点归属旧方",
          demo.get("inflight", {}).get("owner") == "旧接入方", demo.get("inflight", {}).get("owner"))

    old_read = call(base, "POST", f"/api/partition_handoff/{demo_id}/trial-read",
                    {"token": demo_tokens.get("旧接入方"), "start": 40, "end": 50})
    check("旧方只能读自己的在途区间",
          old_read.get("read_offsets") == [40, 41, 42, 43, 44, 45],
          old_read.get("read_offsets"))
    check("旧方有效断点停在 46", old_read.get("effective_breakpoint") == 46,
          old_read.get("effective_breakpoint"))

    new_read = call(base, "POST", f"/api/partition_handoff/{demo_id}/trial-read",
                    {"token": demo_tokens.get("新接入方"), "start": 40, "end": 50})
    check("新方只读可接续历史",
          new_read.get("read_offsets") == [46, 47, 48, 49, 50], new_read.get("read_offsets"))
    check("新方读不到旧方在途数据",
          new_read.get("denied_offsets") == [40, 41, 42, 43, 44, 45], new_read.get("denied_offsets"))
    check("无法认领区间与拒绝原因一致",
          new_read.get("unclaimable_offsets") == [40, 41, 42, 43, 44, 45],
          new_read.get("unclaimable_offsets"))

    forged = call(base, "POST", f"/api/partition_handoff/{demo_id}/trial-read",
                  {"token": "forged-token", "start": 40, "end": 50})
    check("伪造凭据无法读取", forged.get("ok") is False, forged.get("message"))

    old_offset = find_offset(base, "orders-legacy-source", "orders-consumer-group", "orders", "0")
    new_offset = find_offset(base, "orders-green-source", "orders-consumer-group", "orders", "0")
    check("新旧源各自持有 orders/0 的偏移量记录",
          old_offset is not None and new_offset is not None
          and old_offset.get("id") != new_offset.get("id"),
          (old_offset or {}).get("id"))
    check("旧源进度没有算到新源",
          new_offset is not None and int(new_offset.get("当前Offset", "-1")) < 46,
          None if new_offset is None else new_offset.get("当前Offset"))

    # --- 第 2 段：另建一条移交，逐步走新旧双方争用 ---
    # 每次运行用带时间戳的源名，避免和上一次运行留下的已确认记录撞车。
    suffix = str(int(time.time() * 1000))[-7:]
    old_source_name = f"billing-legacy-{suffix}"
    new_source_name = f"billing-green-{suffix}"
    created, missing = call(base, "POST", "/api/partition_handoff", {"values": {
        "来源源": old_source_name,
        "目标源": new_source_name,
        "消费组": "billing-group",
        "Topic/Queue": "billing",
        "分区/分片": "2",
        "旧方归属区间": "40-45",
        "允许接续区间": "46-50",
        "允许接续边界": "50",
    }}).get("entry"), None
    check("登记分区移交记录", created is not None, created)
    if created is None:
        print("无法继续第 2 段校验")
        return 1
    handoff_id = int(created["id"])
    check("新登记记录处于待受理", created.get("status") == "待受理", created.get("status"))

    accepted = call(base, "POST", f"/api/partition_handoff/{handoff_id}/accept", {})
    check("受理移交并签发凭据", accepted.get("ok") is True and len(accepted.get("credentials", [])) == 2,
          [item.get("role") for item in accepted.get("credentials", [])])
    tokens = {item.get("role"): item.get("token") for item in accepted.get("credentials", [])}
    old_token, new_token = tokens.get("旧接入方"), tokens.get("新接入方")

    inflight = call(base, "POST", f"/api/partition_handoff/{handoff_id}/inflight",
                    {"token": old_token, "start": 40, "end": 45})
    check("旧方提交在途结果 40–45", inflight.get("ok") is True, inflight.get("message"))

    conflict = call(base, "POST", f"/api/partition_handoff/{handoff_id}/confirm", {})
    check("确认移交与旧方在途冲突时暂停",
          conflict.get("ok") is False and conflict.get("conflict") is True
          and conflict.get("entry", {}).get("status") == "待旧方收尾", conflict.get("message"))

    new_try_old = call(base, "POST", "/api/record_checkpoint", {"values": {
        "关联接入任务": f"{new_source_name}@billing/2#billing-group",
        "检查点类型": "分区移交-新方读取",
        "检查点值": "40-45",
        "来源源": new_source_name,
        "消费组": "billing-group",
        "Topic/Queue": "billing",
        "分区/分片": "2",
        "移交记录": str(handoff_id),
    }}).get("entry")
    denied = call(base, "POST",
                  f"/api/record_checkpoint/{new_try_old['id']}/confirm-with-credential",
                  {"token": new_token})
    check("新方不能确认旧方在途区间",
          denied.get("ok") is False and denied.get("owner") == "旧接入方", denied.get("message"))

    resolved = call(base, "POST", f"/api/partition_handoff/{handoff_id}/resolve-inflight",
                    {"token": old_token})
    check("旧方凭据收尾在途结果", resolved.get("ok") is True, resolved.get("message"))
    old_bp = resolved.get("effective_breakpoint")
    check("旧方收尾后有效断点为 46", old_bp == 46, old_bp)

    confirmed = call(base, "POST", f"/api/partition_handoff/{handoff_id}/confirm", {})
    check("旧方收尾后移交可确认", confirmed.get("ok") is True, confirmed.get("message"))

    new_cp = call(base, "POST", "/api/record_checkpoint", {"values": {
        "关联接入任务": f"{new_source_name}@billing/2#billing-group",
        "检查点类型": "分区移交-新方读取",
        "检查点值": "46-50",
        "来源源": new_source_name,
        "消费组": "billing-group",
        "Topic/Queue": "billing",
        "分区/分片": "2",
        "移交记录": str(handoff_id),
    }}).get("entry")
    new_ok = call(base, "POST",
                  f"/api/record_checkpoint/{new_cp['id']}/confirm-with-credential",
                  {"token": new_token})
    check("新方确认可接续区间", new_ok.get("ok") is True, new_ok.get("message"))
    check("新方有效断点推进到 51", new_ok.get("effective_breakpoint") == 51,
          new_ok.get("effective_breakpoint"))

    old_cp = call(base, "POST", "/api/record_checkpoint", {"values": {
        "关联接入任务": f"{old_source_name}@billing/2#billing-group",
        "检查点类型": "分区移交-旧方复核",
        "检查点值": "46-50",
        "来源源": old_source_name,
        "消费组": "billing-group",
        "Topic/Queue": "billing",
        "分区/分片": "2",
        "移交记录": str(handoff_id),
    }}).get("entry")
    old_denied = call(base, "POST",
                      f"/api/record_checkpoint/{old_cp['id']}/confirm-with-credential",
                      {"token": old_token})
    check("旧方不能回头确认已移交区间", old_denied.get("ok") is False, old_denied.get("message"))

    old_dup = call(base, "POST", f"/api/partition_handoff/{handoff_id}/inflight",
                   {"token": old_token, "start": 40, "end": 45})
    check("旧方重复提交在途结果保持幂等", old_dup.get("ok") is True, old_dup.get("message"))

    generic = call(base, "POST", f"/api/record_checkpoint/{new_cp['id']}/actions",
                   {"action": "确认检查点"})
    check("通用动作不能绕过凭据确认已绑定检查点", generic.get("ok") is False, generic.get("message"))

    history = call(base, "POST", "/api/record_checkpoint/available-history", {"token": new_token})
    check("新方可采用历史只到边界",
          history.get("available_range") == "46-50" and history.get("boundary") == 50,
          history.get("available_range"))
    check("新方历史说明包含旧方在途区间",
          history.get("old_inflight_range") == "40-45", history.get("old_inflight_range"))

    old_offset = find_offset(base, old_source_name, "billing-group", "billing", "2")
    new_offset = find_offset(base, new_source_name, "billing-group", "billing", "2")
    check("billing/2 旧源偏移为 46", old_offset is not None and old_offset.get("当前Offset") == "46",
          None if old_offset is None else old_offset.get("当前Offset"))
    check("billing/2 新源偏移为 51", new_offset is not None and new_offset.get("当前Offset") == "51",
          None if new_offset is None else new_offset.get("当前Offset"))

    claims = call(base, "GET", f"/api/partition_handoff/{handoff_id}/summary").get("claims", [])
    ranges = sorted((claim.get("role"), claim.get("起点"), claim.get("终点"),
                     claim.get("state")) for claim in claims)
    check("认领账本没有重复区间", len({(role, start, end) for role, start, end, _ in ranges}) == len(ranges),
          ranges)

    print()
    if FAILURES:
        print(f"{len(FAILURES)} 项失败：" + "、".join(FAILURES))
        return 1
    print("全部校验通过")
    return 0


if __name__ == "__main__":
    sys.exit(main())
