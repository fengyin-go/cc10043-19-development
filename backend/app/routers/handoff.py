"""分区移交接口：受理、确认、凭据试读、旧方在途结果和检查点确认。

这些接口补在通用资源路由之前注册，避免 `/api/{module}/{entry_id}` 的动态段
吞掉 `demo`、`summary` 这类固定动作路径。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.schemas import ActionPayload, ActionResult
from app.services import service_for
from app.services.partition_handoff import PartitionHandoffService

router = APIRouter(prefix="/api", tags=["分区移交"])


class HandoffActionPayload(BaseModel):
    token: str | None = None
    start: int | None = None
    end: int | None = None
    force: bool = False
    reason: str | None = None


class CredentialPayload(BaseModel):
    token: str | None = None
    start: int | None = None
    end: int | None = None


class TrialReadPayload(CredentialPayload):
    pass


def handoff_service() -> PartitionHandoffService:
    service = service_for("partition_handoff")
    assert isinstance(service, PartitionHandoffService)
    return service


@router.post("/partition_handoff/demo")
def demo_scenario() -> dict[str, Any]:
    """搭出 orders/0 的双源移交场景，并返回新旧凭据。"""
    return handoff_service().demo_scenario()


@router.get("/partition_handoff/{handoff_id}/summary")
def handoff_summary(handoff_id: int) -> dict[str, Any]:
    result = handoff_service().summary(handoff_id)
    if not result.get("ok"):
        raise HTTPException(status_code=404, detail=str(result.get("message")))
    return result


@router.get("/partition_handoff/{handoff_id}/ownership")
def handoff_ownership(handoff_id: int) -> dict[str, Any]:
    service = handoff_service()
    entry = service.get_entry(handoff_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"分区移交记录 {handoff_id} 不存在")
    ownership = [item for item in service.ownership() if int(item["handoff_id"]) == handoff_id]
    return {"ok": True, "entry": entry, "ownership": ownership[0] if ownership else None}


@router.post("/partition_handoff/{handoff_id}/accept")
def accept_handoff(handoff_id: int, payload: HandoffActionPayload | None = None) -> dict[str, Any]:
    return handoff_service().accept_handoff(handoff_id)


@router.post("/partition_handoff/{handoff_id}/confirm")
def confirm_handoff(handoff_id: int, payload: HandoffActionPayload | None = None) -> dict[str, Any]:
    force = bool(payload.force) if payload is not None else False
    return handoff_service().confirm_handoff(handoff_id, force=force)


@router.post("/partition_handoff/{handoff_id}/reject")
def reject_handoff(handoff_id: int, payload: HandoffActionPayload | None = None) -> dict[str, Any]:
    reason = str(payload.reason or "") if payload is not None else ""
    return handoff_service().reject_handoff(handoff_id, reason)


@router.post("/partition_handoff/{handoff_id}/actions", response_model=ActionResult)
def run_handoff_action(handoff_id: int, payload: ActionPayload) -> ActionResult:
    """通用动作入口：ok 以领域结果为准，冲突时不能只因为记录存在就报成功。"""
    service = handoff_service()
    action = payload.action.strip()
    if action in ("受理移交", "受理", "accept"):
        result = service.accept_handoff(handoff_id)
    elif action in ("确认移交", "确认", "confirm"):
        result = service.confirm_handoff(handoff_id)
    elif action in ("驳回移交", "驳回", "reject"):
        result = service.reject_handoff(handoff_id)
    else:
        result = {"ok": False, "message": f"动作「{action}」不属于分区移交可执行范围"}
    return ActionResult(
        ok=bool(result.get("ok")),
        message=str(result.get("message", "")),
        entry=result.get("entry"),
    )


@router.post("/partition_handoff/{handoff_id}/inflight")
def submit_inflight(handoff_id: int, payload: HandoffActionPayload) -> dict[str, Any]:
    return handoff_service().submit_inflight_result(
        handoff_id, payload.token, payload.start, payload.end)

@router.post("/partition_handoff/{handoff_id}/resolve-inflight")
def resolve_inflight(handoff_id: int, payload: HandoffActionPayload | None = None) -> dict[str, Any]:
    token = payload.token if payload is not None else None
    return handoff_service().resolve_old_inflight(handoff_id, token)


@router.post("/partition_handoff/{handoff_id}/trial-read")
def trial_read(handoff_id: int, payload: TrialReadPayload | None = None) -> dict[str, Any]:
    payload = payload or TrialReadPayload()
    return handoff_service().trial_read(handoff_id, payload.token, payload.start, payload.end)


@router.post("/record_checkpoint/available-history")
def available_history(payload: CredentialPayload) -> dict[str, Any]:
    service = service_for("record_checkpoint")
    if service is None:
        raise HTTPException(status_code=404, detail="记录检查点模块不存在")
    return service.available_history(payload.token)


@router.post("/record_checkpoint/{entry_id}/confirm-with-credential")
def confirm_with_credential(entry_id: int, payload: CredentialPayload) -> dict[str, Any]:
    service = service_for("record_checkpoint")
    if service is None:
        raise HTTPException(status_code=404, detail="记录检查点模块不存在")
    return service.confirm_with_credential(entry_id, payload.token, payload.start, payload.end)


@router.post("/record_checkpoint/trial-read")
def checkpoint_trial_read(payload: TrialReadPayload) -> dict[str, Any]:
    service = service_for("record_checkpoint")
    if service is None:
        raise HTTPException(status_code=404, detail="记录检查点模块不存在")
    return service.trial_read(payload.token, payload.start, payload.end)
