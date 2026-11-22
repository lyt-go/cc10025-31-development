"""补数兼容版本索引接口：历史分区字段需求与旧Schema的查询比较、字段校验、版本提交与执行记录复查。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.schemas import ActionResult
from app.services import service_for

router = APIRouter(prefix="/api/backfill-compat", tags=["补数兼容版本索引"])


class ValidatePayload(BaseModel):
    asset_id: str
    partition: str
    version_id: str
    required: str
    verified_extra: list[str] = Field(default_factory=list)


class BindPayload(BaseModel):
    job_id: int
    version_id: str
    verified_extra: list[str] = Field(default_factory=list)


@router.get("/candidates", response_model=dict)
def compat_candidates(
    asset_id: str = Query(..., description="目标资产"),
    partition: str = Query(..., description="来源分区，如 dt=2026-09-01"),
    required: str = Query(..., description="旧分区字段需求，如 user_id:bigint,dt:string"),
) -> dict[str, Any]:
    """按旧分区的字段要求过滤候选版本，返回补数兼容版本索引（含版本排序、覆盖列、核验结果）。"""
    service = service_for("schema_registry")
    return service.build_backfill_compat_index(asset_id, partition, required)


@router.post("/validate", response_model=dict)
def validate_candidate(payload: ValidatePayload) -> dict[str, Any]:
    """值班人字段校验：核定候选版本对该分区可写入的列，未经验证的列不进结果。"""
    service = service_for("schema_registry")
    result, message = service.validate_candidate(
        payload.asset_id,
        payload.partition,
        payload.version_id,
        payload.required,
        payload.verified_extra,
    )
    if result is None:
        raise HTTPException(status_code=404, detail=message)
    return result


@router.post("/bind", response_model=ActionResult)
def bind_version(payload: BindPayload) -> ActionResult:
    """值班人提交实际读取的版本：校验通过后把版本与可写入字段清单固定到补数作业。"""
    service = service_for("backfill_job")
    entry, message = service.bind_schema_version(
        payload.job_id, payload.version_id, payload.verified_extra)
    return ActionResult(ok=entry is not None, message=message, entry=entry)


@router.get("/jobs/{job_id}/execution", response_model=dict)
def execution_record(job_id: int) -> dict[str, Any]:
    """复查补数执行记录：所用版本、实际写入字段清单、来源分区与历次执行。"""
    service = service_for("backfill_job")
    record = service.get_execution_record(job_id)
    if record is None:
        raise HTTPException(status_code=404, detail=f"补数作业 {job_id} 暂无执行记录")
    return record
