"""schema_registry 领域服务：Schema注册管理，接收 asset_catalog 上游事件。

跨模块数据流：
    asset_catalog → schema_registry: 资产发布触发Schema注册
    schema_registry → version_snapshot: Schema注册触发版本快照创建

补数兼容版本索引：
    历史补数不能只按资产取当前版本。这里把历史分区的字段需求与全部已注册
    旧版本逐一比较，生成可定位可用结构的补数兼容版本索引；值班人基于索引
    结果做字段校验，再提交实际读取的版本。
"""
from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import store

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "assets")


def _partition_token(partition: str) -> str:
    """把 dt=2026-09-01 这类分区标识转成可用于文件名的安全片段。"""
    return re.sub(r"[^0-9A-Za-z_.-]+", "_", str(partition)).strip("_") or "unknown"


class SchemaRegistryService(BaseModuleService):
    MODULE = "schema_registry"
    ENTITY = "Schema版本"
    OPERATION_TERM = "入库"
    LIST_FIELDS = ["版本编号", "关联资产", "字段清单", "前向兼容", "后向兼容", "Schema摘要", "注册时间", "版本状态"]
    REQUIRED_FIELDS = ["版本编号", "关联资产", "字段清单"]
    STATUS_ORDER = ["草稿", "待校验", "已注册", "已废止"]
    ACTION_RULES = {"提交校验": "待校验", "确认注册": "已注册", "标记废止": "已废止"}
    NEGATIVE_ACTIONS = []

    def __init__(self):
        super().__init__()
        os.makedirs(DATA_DIR, exist_ok=True)

    # ---------- 字段清单解析 ----------

    @staticmethod
    def parse_field_list(text: str) -> list[dict[str, str]]:
        """把「name:type,name:type」字段清单解析成结构化列表。

        类型里允许出现括号参数（如 decimal(18,2)），括号内的逗号不参与切分。
        """
        tokens: list[str] = []
        buf: list[str] = []
        depth = 0
        for ch in str(text or ""):
            if ch in "([{":
                depth += 1
            elif ch in ")]}":
                depth = max(0, depth - 1)
            if ch in ",,;;" and depth == 0:
                tokens.append("".join(buf))
                buf = []
            else:
                buf.append(ch)
        tokens.append("".join(buf))

        fields: list[dict[str, str]] = []
        for token in tokens:
            token = token.strip()
            if not token:
                continue
            name, sep, ftype = token.replace(":", ":").partition(":")
            fields.append({
                "name": name.strip(),
                "type": ftype.strip() if sep else "unknown",
            })
        return fields

    # ---------- 版本历史与查询比较 ----------

    def registered_versions(self, asset_id: str) -> list[dict[str, Any]]:
        """资产的全部已注册版本（草稿/待校验/已废止不进候选），按注册时间升序。"""
        rows = [
            row for row in store.rows(self.MODULE)
            if str(row.get("关联资产", "")) == asset_id and row.get("status") == "已注册"
        ]
        return sorted(
            rows,
            key=lambda row: (str(row.get("注册时间", "")), str(row.get("版本编号", ""))),
        )

    @staticmethod
    def _compare_with_requirement(
        version_fields: list[dict[str, str]],
        required_fields: list[dict[str, str]],
    ) -> dict[str, Any]:
        """把单个版本的字段清单与分区字段需求逐列比较。"""
        required_by_name = {field["name"]: field["type"] for field in required_fields}
        version_by_name = {field["name"]: field["type"] for field in version_fields}
        covered: list[dict[str, str]] = []
        missing: list[dict[str, str]] = []
        type_mismatch: list[dict[str, str]] = []
        for req in required_fields:
            name, req_type = req["name"], req["type"]
            if name not in version_by_name:
                missing.append(req)
            elif req_type != "unknown" and version_by_name[name] != req_type:
                type_mismatch.append({
                    "name": name,
                    "require_type": req_type,
                    "version_type": version_by_name[name],
                })
            else:
                covered.append(req)
        derived = [field for field in version_fields if field["name"] not in required_by_name]
        return {
            "covered": covered,
            "missing": missing,
            "type_mismatch": type_mismatch,
            "derived": derived,
        }

    def build_backfill_compat_index(
        self,
        asset_id: str,
        partition: str,
        required_text: str,
    ) -> dict[str, Any]:
        """生成补数兼容版本索引：按旧分区字段需求过滤候选并排序。

        排序规则：可用版本优先；可用版本里衍生列越少越靠前（恢复原字段的版本
        最贴近历史结构），再按注册时间升序；不可用版本排最后并给出原因。
        """
        required = self.parse_field_list(required_text)
        candidates: list[dict[str, Any]] = []
        for row in self.registered_versions(asset_id):
            version_fields = self.parse_field_list(str(row.get("字段清单", "")))
            compared = self._compare_with_requirement(version_fields, required)
            usable = not compared["missing"] and not compared["type_mismatch"]
            if not usable:
                match_kind = "不可用"
                semantics = "不满足该分区字段需求，不能用于补数"
            elif compared["derived"]:
                match_kind = "旧字段+衍生列"
                semantics = (
                    f"结果集含{len(compared['derived'])}个衍生列，未核验列不写入；"
                    "与恢复原字段版本的消费口径可能冲突"
                )
            else:
                match_kind = "恢复原字段"
                semantics = "结果集即历史分区原字段，消费口径与当时一致"

            required_check = (
                [{"name": f["name"], "type": f["type"], "核验": "已核验", "说明": "类型一致"}
                 for f in compared["covered"]]
                + [{"name": f["name"], "type": f["type"], "核验": "缺失", "说明": "版本中没有该列"}
                   for f in compared["missing"]]
                + [{"name": m["name"], "核验": "类型不符",
                    "说明": f"需求{m['require_type']}，版本为{m['version_type']}"}
                   for m in compared["type_mismatch"]]
            )
            candidates.append({
                "版本编号": str(row.get("版本编号", "")),
                "注册时间": str(row.get("注册时间", "")),
                "匹配方式": match_kind,
                "可用": usable,
                "覆盖列": [f["name"] for f in compared["covered"]],
                "缺失列": [f["name"] for f in compared["missing"]],
                "类型不符列": compared["type_mismatch"],
                "衍生列": compared["derived"],
                "核验结果": {
                    "需求列核验": "通过" if usable else "不通过",
                    "需求列明细": required_check,
                    "衍生列核验": ("未核验" if compared["derived"] else "无衍生列"),
                },
                "口径提示": semantics,
            })

        candidates.sort(key=lambda c: (
            not c["可用"],
            len(c["衍生列"]),
            str(c["注册时间"]),
            str(c["版本编号"]),
        ))
        index = {
            "asset_id": asset_id,
            "partition": partition,
            "required_fields": required,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "排序说明": "可用版本优先；同类按衍生列数升序，再按注册时间升序；不可用版本置后",
            "candidates": candidates,
        }
        self._write_compat_index(asset_id, partition, index)
        return index

    # ---------- 字段校验 ----------

    def validate_candidate(
        self,
        asset_id: str,
        partition: str,
        version_id: str,
        required_text: str,
        verified_extra: list[str] | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        """值班人字段校验：核定候选版本对该分区可写入的列。

        需求列必须全部覆盖且类型一致才判通过；衍生列默认未核验，只有值班人
        显式确认（verified_extra）的衍生列才进入可写入字段清单，未经验证的
        列不会出现在补数结果里。
        """
        required = self.parse_field_list(required_text)
        row = next(
            (r for r in self.registered_versions(asset_id)
             if str(r.get("版本编号", "")) == version_id),
            None,
        )
        if row is None:
            return None, f"Schema版本 {version_id} 不存在或未注册，不能用于补数"

        verified_extra = list(verified_extra or [])
        version_fields = self.parse_field_list(str(row.get("字段清单", "")))
        compared = self._compare_with_requirement(version_fields, required)
        derived_names = {f["name"] for f in compared["derived"]}

        columns: list[dict[str, Any]] = []
        writable: list[dict[str, str]] = []
        for field in compared["covered"]:
            columns.append({"name": field["name"], "type": field["type"],
                            "来源": "需求列", "核验": "已核验", "说明": "类型一致"})
            writable.append(field)
        for field in compared["missing"]:
            columns.append({"name": field["name"], "type": field["type"],
                            "来源": "需求列", "核验": "缺失", "说明": "版本中没有该列"})
        for item in compared["type_mismatch"]:
            columns.append({"name": item["name"], "来源": "需求列", "核验": "类型不符",
                            "说明": f"需求{item['require_type']}，版本为{item['version_type']}"})
        for field in compared["derived"]:
            if field["name"] in verified_extra:
                columns.append({"name": field["name"], "type": field["type"], "来源": "衍生列",
                                "核验": "已核验", "说明": "值班人确认该衍生列可用于此分区"})
                writable.append(field)
            else:
                columns.append({"name": field["name"], "type": field["type"], "来源": "衍生列",
                                "核验": "未核验", "说明": "未经验证，不写入补数结果"})

        passed = not compared["missing"] and not compared["type_mismatch"]
        unmatched = [name for name in verified_extra if name not in derived_names]
        if compared["derived"]:
            semantics = (
                f"该版本含{len(compared['derived'])}个衍生列，本次核验放行"
                f"{sum(1 for f in compared['derived'] if f['name'] in verified_extra)}个；"
                "与恢复原字段版本的消费口径可能冲突"
            )
        else:
            semantics = "结果集即历史分区原字段，消费口径与当时一致"

        result = {
            "asset_id": asset_id,
            "partition": partition,
            "version_id": version_id,
            "结果": "通过" if passed else "不通过",
            "columns": columns,
            "可写入字段清单": writable if passed else [],
            "verified_extra": verified_extra,
            "未匹配的核验列": unmatched,
            "口径提示": semantics,
            "validated_at": datetime.now(timezone.utc).isoformat(),
        }
        self._write_validation_record(asset_id, partition, version_id, result)
        if not passed:
            reasons = [c["name"] for c in columns if c["核验"] in ("缺失", "类型不符")]
            return result, f"字段校验不通过：{'、'.join(reasons)}"
        return result, f"字段校验通过，可写入 {len(writable)} 列"

    # ---------- 既有注册动作 ----------

    def register_schema(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在"

        asset_id = str(entry.get("关联资产", ""))
        fields = str(entry.get("字段清单", ""))
        forward_ok = str(entry.get("前向兼容", ""))
        backward_ok = str(entry.get("后向兼容", ""))

        entry["status"] = "已注册"
        entry["pending"] = False
        entry["注册时间"] = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        entry["版本状态"] = "已注册"

        # 写入Schema注册文件
        self._write_schema_record(asset_id, entry)

        # 创建版本快照
        self._create_version_snapshot(asset_id, entry)

        return entry, f"{self.ENTITY} {entry_id} 已注册: {fields}"

    def get_schema_for_asset(self, asset_id: str) -> dict[str, Any] | None:
        filename = f"schema_{asset_id}.json"
        path = os.path.join(DATA_DIR, filename)
        if not os.path.exists(path):
            return None
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        if action == "确认注册":
            return self.register_schema(entry_id)
        return super().run_action(entry_id, action)

    def _write_schema_record(self, asset_id: str, entry: dict[str, Any]) -> None:
        filename = f"schema_{asset_id}.json"
        path = os.path.join(DATA_DIR, filename)
        record = {
            "schema_id": str(entry.get("版本编号", "")),
            "asset_id": asset_id,
            "fields": str(entry.get("字段清单", "")),
            "forward_compatible": str(entry.get("前向兼容", "")),
            "backward_compatible": str(entry.get("后向兼容", "")),
            "summary": str(entry.get("Schema摘要", "")),
            "registered_at": datetime.now(timezone.utc).isoformat(),
            "version_status": "已注册",
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2, default=str)

    def _write_compat_index(self, asset_id: str, partition: str, index: dict[str, Any]) -> None:
        """补数兼容版本索引落盘：让历史分区随时能定位可用结构。"""
        filename = f"backfill_compat_index_{asset_id}_{_partition_token(partition)}.json"
        path = os.path.join(DATA_DIR, filename)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(index, f, ensure_ascii=False, indent=2, default=str)

    def _write_validation_record(
        self,
        asset_id: str,
        partition: str,
        version_id: str,
        result: dict[str, Any],
    ) -> None:
        """字段校验结果落盘：值班人的核验结论留痕，供补数执行复核。"""
        filename = f"backfill_validation_{asset_id}_{_partition_token(partition)}_{version_id}.json"
        path = os.path.join(DATA_DIR, filename)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2, default=str)

    def _create_version_snapshot(self, asset_id: str, schema_entry: dict[str, Any]) -> None:
        snap_rows = store.rows("version_snapshot")
        next_id = max((int(r.get("id", 0)) for r in snap_rows), default=0) + 1
        snap = {
            "id": next_id,
            "status": "已完成",
            "pending": False,
            "abnormal": False,
            "快照编号": f"VERS-{next_id:04d}",
            "关联资产": asset_id,
            "快照时间": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "数据量": "0MB",
            "快照类型": "Schema快照",
            "保留天数": "90",
            "依赖快照": "",
            "快照状态": "已完成",
        }
        snap_rows.append(snap)