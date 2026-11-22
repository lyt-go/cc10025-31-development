"""backfill_job 领域服务：补数任务管理，接收 partition_plan 上游事件。

跨模块数据流：
    partition_plan → backfill_job: 分区方案生效触发补数
    backfill_job → cost_tracker: 补数任务更新计算成本
    schema_registry → backfill_job: 补数兼容版本索引核定补数所用Schema版本

补数版本管理：
    值班人按补数兼容版本索引完成字段校验后，把实际读取的版本提交到作业上；
    执行时按索引核定该版本仍可用，并把实际写入的字段清单和来源分区一起记入
    执行记录。已提交的作业始终按当时选择的版本重跑，不受后续新注册版本影响。
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import store

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "assets")


class BackfillJobService(BaseModuleService):
    MODULE = "backfill_job"
    ENTITY = "补数作业"
    OPERATION_TERM = "回填"
    LIST_FIELDS = ["作业编号", "目标资产", "补数区间", "数据量", "依赖检查", "优先级", "重试次数", "作业状态"]
    REQUIRED_FIELDS = ["作业编号", "目标资产", "补数区间"]
    STATUS_ORDER = ["排队中", "执行中", "已完成", "已失败"]
    ACTION_RULES = {"发起执行": "执行中", "确认完成": "已完成", "标记失败": "已失败"}
    NEGATIVE_ACTIONS = []

    def __init__(self):
        super().__init__()
        os.makedirs(DATA_DIR, exist_ok=True)

    # ---------- 版本提交（值班人） ----------

    def bind_schema_version(
        self,
        entry_id: int,
        version_id: str,
        verified_extra: list[str] | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        """值班人提交实际读取的Schema版本。

        先按作业的来源分区和字段需求重新生成补数兼容版本索引并完成字段校验，
        校验通过才把版本和可写入字段清单固定到作业上；未经验证的列不会进入
        核验字段清单。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在"

        asset_id = str(entry.get("目标资产", ""))
        partition = str(entry.get("来源分区", ""))
        requirement = str(entry.get("字段需求", ""))
        if not partition or not requirement:
            return None, f"{self.ENTITY} {entry_id} 缺少来源分区或字段需求，无法提交读取版本"

        from app.services import service_for  # 延迟导入，避免服务容器循环依赖
        schema_svc = service_for("schema_registry")
        result, message = schema_svc.validate_candidate(
            asset_id, partition, version_id, requirement, verified_extra)
        if result is None:
            return None, message
        if result["结果"] != "通过":
            return None, f"版本 {version_id} {message}，不能提交为读取版本"

        entry["使用版本"] = version_id
        entry["核验字段清单"] = result["可写入字段清单"]
        entry["版本绑定时间"] = result["validated_at"]
        entry["口径提示"] = result["口径提示"]
        writable = len(result["可写入字段清单"])
        return entry, f"{self.ENTITY} {entry_id} 已提交读取版本 {version_id}，可写入 {writable} 列"

    # ---------- 执行与重跑 ----------

    def start_backfill(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在"

        asset_id = str(entry.get("目标资产", ""))
        interval = str(entry.get("补数区间", ""))

        execution = self._check_version_for_run(entry)
        if execution is None and str(entry.get("字段需求", "")):
            version = str(entry.get("使用版本", ""))
            if not version:
                return None, (f"{self.ENTITY} {entry_id} 尚未提交实际读取的Schema版本，"
                              "请先按补数兼容版本索引完成字段校验并绑定")
            return None, (f"{self.ENTITY} {entry_id} 绑定的版本 {version} "
                          "已不在补数兼容版本索引的可用候选中，需重新校验绑定")

        entry["status"] = "执行中"
        entry["pending"] = True
        entry["作业状态"] = "执行中"

        # 写入补数记录（含所用版本、实际写入字段清单、来源分区）
        self._write_backfill_record(asset_id, entry, execution=execution)

        # 更新成本追踪中的计算成本
        self._update_cost_for_backfill(asset_id)

        return entry, f"{self.ENTITY} {entry_id} 执行中，{interval}"

    def _check_version_for_run(self, entry: dict[str, Any]) -> dict[str, Any] | None:
        """按补数兼容版本索引核定所用版本，返回本次执行记录片段。

        作业带有字段需求时：必须已提交读取版本，且该版本当前仍在索引的可用
        候选中；实际写入的字段清单取绑定时核验通过的列，保证未经验证的列不
        出现在结果里。重跑沿用当时选择的版本，不重新挑选。
        """
        requirement = str(entry.get("字段需求", ""))
        partition = str(entry.get("来源分区", ""))
        if not requirement or not partition:
            return None
        version = str(entry.get("使用版本", ""))
        if not version:
            return None

        from app.services import service_for  # 延迟导入，避免服务容器循环依赖
        schema_svc = service_for("schema_registry")
        index = schema_svc.build_backfill_compat_index(
            str(entry.get("目标资产", "")), partition, requirement)
        candidate = next(
            (c for c in index["candidates"] if c["版本编号"] == version), None)
        if candidate is None or not candidate["可用"]:
            return None

        return {
            "executed_at": datetime.now(timezone.utc).isoformat(),
            "schema_version": version,
            "written_fields": list(entry.get("核验字段清单") or []),
            "source_partition": partition,
            "核定方式": "补数兼容版本索引",
        }

    def complete_backfill(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在"

        asset_id = str(entry.get("目标资产", ""))
        entry["status"] = "已完成"
        entry["pending"] = False
        entry["作业状态"] = "已完成"

        self._write_backfill_record(asset_id, entry)

        return entry, f"{self.ENTITY} {entry_id} 已完成"

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        if action == "发起执行":
            return self.start_backfill(entry_id)
        if action == "确认完成":
            return self.complete_backfill(entry_id)
        return super().run_action(entry_id, action)

    # ---------- 执行记录 ----------

    def get_execution_record(self, entry_id: int) -> dict[str, Any] | None:
        """复查补数执行记录：所用版本、实际写入字段清单、来源分区与历次执行。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None
        path = self._record_path(entry)
        if not os.path.exists(path):
            return None
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _record_path(self, entry: dict[str, Any]) -> str:
        job_id = str(entry.get("作业编号", ""))
        asset_id = str(entry.get("目标资产", ""))
        return os.path.join(DATA_DIR, f"backfill_{job_id}_{asset_id}.json")

    def _write_backfill_record(
        self,
        asset_id: str,
        entry: dict[str, Any],
        execution: dict[str, Any] | None = None,
    ) -> None:
        path = self._record_path(entry)
        record: dict[str, Any] = {}
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    record = json.load(f)
            except (json.JSONDecodeError, OSError):
                record = {}

        record.update({
            "job_id": str(entry.get("作业编号", "")),
            "asset_id": asset_id,
            "interval": str(entry.get("补数区间", "")),
            "data_volume": str(entry.get("数据量", "")),
            "dependency_check": str(entry.get("依赖检查", "")),
            "priority": str(entry.get("优先级", "")),
            "retry_count": str(entry.get("重试次数", "")),
            "status": str(entry.get("作业状态", "")),
            "source_partition": str(entry.get("来源分区", "")),
            "field_requirement": str(entry.get("字段需求", "")),
            "schema_version": str(entry.get("使用版本", "")),
            "written_fields": entry.get("核验字段清单") or [],
            "updated_at": datetime.now(timezone.utc).isoformat(),
        })
        executions = record.get("executions")
        if not isinstance(executions, list):
            executions = []
        if execution is not None:
            executions.append(execution)
        record["executions"] = executions

        with open(path, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2, default=str)

    def _update_cost_for_backfill(self, asset_id: str) -> None:
        cost_rows = store.rows("cost_tracker")
        for row in cost_rows:
            if str(row.get("关联资产")) == asset_id:
                row["计算成本"] = "补数任务执行中"
                row["status"] = "已核算"
                row["pending"] = False
                break
