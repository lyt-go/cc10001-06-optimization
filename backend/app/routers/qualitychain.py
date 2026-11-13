"""质量链路接口：贯通解析、校验与管道处理，暴露运行、原始记录、隔离区与管道产出查询。"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services.qualitychain import qualitychain

router = APIRouter(prefix="/api/qualitychain", tags=["质量链路"])

service = qualitychain


@router.post("/run", response_model=ActionResult)
def run_chain(payload: EntryPayload) -> ActionResult:
    """按任务（或管道）运行一次完整质量链路。

    返回逐阶段统计与逐条结果，配置缺环时说明具体缺哪一环，不返回笼统成功。
    """
    values = payload.values or {}
    report, message = service.run_chain(
        task_code=str(values.get("任务编号") or "").strip() or None,
        pipeline_code=str(values.get("管道编号") or "").strip() or None,
        rule_code=str(values.get("规则编号") or "").strip() or None,
        check_code=str(values.get("校验编号") or "").strip() or None,
    )
    if report is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=report)


@router.get("/raw", response_model=PageResult[dict])
def list_raw(
    task_code: str | None = Query(default=None, alias="任务编号", description="按任务编号过滤原始记录"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """查看原始记录及每条记录的质量步骤完成情况（已清洗/已校验/已隔离/已入管道）。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_raw(task_code, page, size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/quarantine", response_model=PageResult[dict])
def list_quarantine(
    task_code: str | None = Query(default=None, alias="任务编号", description="按任务编号过滤"),
    pipeline_code: str | None = Query(default=None, alias="管道编号", description="按管道编号过滤"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """查看异常隔离区：每条失败行保留原始 JSON、失败字段与失败原因。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_quarantine(task_code, pipeline_code, page, size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/output", response_model=PageResult[dict])
def list_output(
    pipeline_code: str | None = Query(default=None, alias="管道编号", description="按管道编号过滤"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """查看已写入 NDJSON 管道的记录（均通过校验且完成去重）。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_output(pipeline_code, page, size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/runs", response_model=PageResult[dict])
def list_runs(page: int = 1, size: int = 20) -> PageResult[dict]:
    """查看质量链路运行台账：每轮的处理量、异常比例与失败原因。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_runs(page, size)
    return PageResult(items=items, total=total, page=page, size=size)
