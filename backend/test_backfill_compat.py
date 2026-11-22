"""补数兼容版本索引集成测试。

走一遍完整验收流程：
1. 按旧分区的字段要求过滤候选，检查版本排序、覆盖列和核验结果；
2. 值班人从搜索结果完成字段校验，提交实际读取的版本；
3. 补数任务按补数兼容版本索引核定所用版本，执行记录写入字段清单与来源分区；
4. 模拟再注册一个 Schema 后复查刚才的补数记录，已有作业仍按当时版本重跑。
"""
from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

from app.store import store
from app.services import service_for

DATA_ROOT = os.path.join(os.path.dirname(__file__), "app", "data")
RED = "\033[91m"
GREEN = "\033[92m"
RESET = "\033[0m"

passed = 0
failed = 0

ASSET = "ASSE-0003"
PARTITION = "dt=2026-09-01"
REQUIRED = "user_id:bigint,user_name:string,dt:string"
JOB_ID = 1  # BACK-0001：种子中带来源分区与字段需求的补数作业


def check(desc, condition, detail=""):
    global passed, failed
    if condition:
        print(f"  {GREEN}PASS{RESET} {desc}")
        passed += 1
    else:
        print(f"  {RED}FAIL{RESET} {desc} {detail}")
        failed += 1
        return False
    return True


def test_candidate_query():
    """按旧分区字段要求过滤候选：版本排序、覆盖列、核验结果。"""
    print("\n=== 步骤1: 查询比较 → 补数兼容版本索引 ===")
    svc = service_for("schema_registry")
    index = svc.build_backfill_compat_index(ASSET, PARTITION, REQUIRED)

    versions = [c["版本编号"] for c in index["candidates"]]
    check("候选只含已注册版本（SCHE-0104待校验不进候选）",
          "SCHE-0104" not in versions, f"候选={versions}")
    check("版本排序：恢复原字段 → 旧字段+衍生列 → 不可用",
          versions == ["SCHE-0101", "SCHE-0102", "SCHE-0103"],
          f"实际={versions}")

    by_version = {c["版本编号"]: c for c in index["candidates"]}
    v1 = by_version["SCHE-0101"]
    check("SCHE-0101 恢复原字段，覆盖全部需求列",
          v1["匹配方式"] == "恢复原字段" and v1["可用"]
          and v1["覆盖列"] == ["user_id", "user_name", "dt"]
          and v1["衍生列"] == [])

    v2 = by_version["SCHE-0102"]
    derived_names = [f["name"] for f in v2["衍生列"]]
    check("SCHE-0102 保有旧字段并兼容新的衍生列",
          v2["匹配方式"] == "旧字段+衍生列" and v2["可用"]
          and derived_names == ["region", "order_gmv"],
          f"衍生列={derived_names}")
    check("SCHE-0102 衍生列默认未核验",
          v2["核验结果"]["衍生列核验"] == "未核验")

    v3 = by_version["SCHE-0103"]
    check("SCHE-0103 类型不符被判不可用",
          (not v3["可用"]) and v3["匹配方式"] == "不可用"
          and v3["类型不符列"][0]["name"] == "user_id"
          and v3["核验结果"]["需求列核验"] == "不通过")

    index_file = os.path.join(
        DATA_ROOT, "assets", f"backfill_compat_index_{ASSET}_dt_2026-09-01.json")
    check("补数兼容版本索引已落盘", os.path.exists(index_file), index_file)


def test_validation():
    """值班人字段校验：衍生列默认不进结果，类型不符/未注册版本被拒绝。"""
    print("\n=== 步骤2: 字段校验 ===")
    svc = service_for("schema_registry")

    result, msg = svc.validate_candidate(ASSET, PARTITION, "SCHE-0102", REQUIRED)
    check("SCHE-0102 校验通过", result is not None and result["结果"] == "通过", msg)
    writable = [f["name"] for f in result["可写入字段清单"]]
    check("未核验衍生列不进可写入清单",
          writable == ["user_id", "user_name", "dt"], f"可写入={writable}")

    result2, _ = svc.validate_candidate(
        ASSET, PARTITION, "SCHE-0102", REQUIRED, verified_extra=["region"])
    writable2 = [f["name"] for f in result2["可写入字段清单"]]
    check("值班人确认region后进入可写入清单，order_gmv仍未核验",
          writable2 == ["user_id", "user_name", "dt", "region"],
          f"可写入={writable2}")

    result3, msg3 = svc.validate_candidate(ASSET, PARTITION, "SCHE-0103", REQUIRED)
    check("SCHE-0103 类型不符校验不通过",
          result3 is not None and result3["结果"] == "不通过", msg3)

    result4, msg4 = svc.validate_candidate(ASSET, PARTITION, "SCHE-0104", REQUIRED)
    check("未注册版本不能校验", result4 is None, msg4)

    validation_file = os.path.join(
        DATA_ROOT, "assets",
        f"backfill_validation_{ASSET}_dt_2026-09-01_SCHE-0102.json")
    check("字段校验结果已落盘", os.path.exists(validation_file))


def test_bind_and_execute():
    """提交实际读取的版本 → 按索引核定执行 → 执行记录含版本/字段清单/来源分区。"""
    print("\n=== 步骤3: 提交版本并执行补数 ===")
    svc = service_for("backfill_job")

    entry, msg = svc.start_backfill(JOB_ID)
    check("未提交版本前不能执行", entry is None, msg)

    entry, msg = svc.bind_schema_version(JOB_ID, "SCHE-0103")
    check("类型不符的版本不能提交", entry is None, msg)

    entry, msg = svc.bind_schema_version(JOB_ID, "SCHE-0102", verified_extra=["region"])
    check("值班人提交实际读取的版本 SCHE-0102", entry is not None, msg)
    if entry:
        check("作业已固定使用版本", entry.get("使用版本") == "SCHE-0102")
        bound = [f["name"] for f in entry.get("核验字段清单", [])]
        check("核验字段清单不含未经验证的order_gmv",
              bound == ["user_id", "user_name", "dt", "region"],
              f"核验字段清单={bound}")

    entry, msg = svc.start_backfill(JOB_ID)
    check("按补数兼容版本索引核定后发起执行", entry is not None, msg)

    record = svc.get_execution_record(JOB_ID)
    check("执行记录已生成", record is not None)
    if record:
        check("执行记录核定版本为 SCHE-0102",
              record.get("schema_version") == "SCHE-0102")
        written = [f["name"] for f in record.get("written_fields", [])]
        check("实际写入字段清单=核验通过列（未经验证的列不出现）",
              written == ["user_id", "user_name", "dt", "region"],
              f"实际写入={written}")
        check("来源分区记入执行记录",
              record.get("source_partition") == PARTITION)
        check("执行历史追加1条且版本一致",
              len(record.get("executions", [])) == 1
              and record["executions"][0]["schema_version"] == "SCHE-0102")

    entry, msg = svc.complete_backfill(JOB_ID)
    check("补数确认完成", entry is not None and entry["status"] == "已完成", msg)
    record = svc.get_execution_record(JOB_ID)
    check("完成后执行记录仍保留版本与执行历史",
          record is not None and record.get("schema_version") == "SCHE-0102"
          and len(record.get("executions", [])) == 1)


def test_reregister_and_recheck():
    """模拟再注册一个 Schema 后复查补数记录：记录不变，重跑仍用当时版本。"""
    print("\n=== 步骤4: 再注册一个Schema后复查补数记录 ===")
    schema_svc = service_for("schema_registry")
    backfill_svc = service_for("backfill_job")

    new_entry, missing = schema_svc.create_entry({
        "版本编号": "SCHE-0105",
        "关联资产": ASSET,
        "字段清单": "user_id:bigint,user_name:string,dt:string,region:string,order_gmv:decimal(18,2),channel:string",
        "前向兼容": "是",
        "后向兼容": "是",
        "Schema摘要": "订单流水v5：新增channel",
        "注册时间": "",
        "版本状态": "待校验",
    })
    check("新Schema版本登记成功", new_entry is not None, f"缺少={missing}")
    entry, msg = schema_svc.register_schema(new_entry["id"])
    check("新Schema注册成功", entry is not None, msg)

    index = schema_svc.build_backfill_compat_index(ASSET, PARTITION, REQUIRED)
    versions = [c["版本编号"] for c in index["candidates"]]
    check("新索引纳入 SCHE-0105（旧字段+衍生列）",
          "SCHE-0105" in versions
          and next(c for c in index["candidates"]
                   if c["版本编号"] == "SCHE-0105")["匹配方式"] == "旧字段+衍生列",
          f"候选={versions}")

    record = backfill_svc.get_execution_record(JOB_ID)
    check("复查：补数记录版本不受新注册影响",
          record is not None and record.get("schema_version") == "SCHE-0102")
    written = [f["name"] for f in record.get("written_fields", [])]
    check("复查：实际写入字段清单不变",
          written == ["user_id", "user_name", "dt", "region"],
          f"实际写入={written}")

    entry, msg = backfill_svc.start_backfill(JOB_ID)
    check("已有作业按当时选择的版本重跑", entry is not None, msg)
    record = backfill_svc.get_execution_record(JOB_ID)
    executions = record.get("executions", []) if record else []
    check("重跑记录仍核定 SCHE-0102，字段清单不变",
          len(executions) == 2
          and all(e["schema_version"] == "SCHE-0102" for e in executions)
          and all([f["name"] for f in e["written_fields"]]
                  == ["user_id", "user_name", "dt", "region"] for e in executions),
          f"执行历史={json.dumps(executions, ensure_ascii=False)}")


if __name__ == "__main__":
    print("=" * 60)
    print("补数兼容版本索引 集成测试")
    print("=" * 60)

    test_candidate_query()
    test_validation()
    test_bind_and_execute()
    test_reregister_and_recheck()

    print(f"\n{'=' * 60}")
    print(f"结果: {GREEN}{passed} 通过{RESET} / {RED}{failed} 失败{RESET} / "
          f"{passed + failed} 总计")
    print("=" * 60)

    if failed > 0:
        sys.exit(1)
