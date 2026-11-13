"""质量链路端到端测试：完整链路、幂等重跑、并发去重、断点续做、配置缺环。

直接运行：cd backend && .venv/bin/python tests/test_qualitychain.py
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
import threading
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ.setdefault("QUALITYCHAIN_DATA_DIR", tempfile.mkdtemp(prefix="qchain-singleton-"))

from app.services.qualitychain import QualityChainService  # noqa: E402
from app.store import store  # noqa: E402

PASS = 0
FAIL = 0


def check(label: str, condition: bool, detail: str = "") -> None:
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  PASS {label}")
    else:
        FAIL += 1
        print(f"  FAIL {label} {detail}")


def fresh_service() -> tuple[QualityChainService, Path]:
    data_dir = Path(tempfile.mkdtemp(prefix="qchain-test-"))
    return QualityChainService(data_dir=data_dir), data_dir


def read_ndjson(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def test_full_chain() -> None:
    print("[test_full_chain] 解析→清洗→校验→隔离→去重→入管道全链路")
    service, data_dir = fresh_service()
    log_before = len(store.rows("storagelog"))
    report, message = service.run_chain(task_code="TASK-117")
    assert report is not None, message

    check("记录总数=13", report["记录总数"] == 13, str(report["记录总数"]))
    check("本次处理=13", report["本次处理"] == 13, str(report["本次处理"]))
    check("入管道=6", report["入管道"] == 6, str(report["入管道"]))
    check("隔离区新增=7", report["隔离区新增"] == 7, str(report["隔离区新增"]))
    check("其中重复=2", report["其中重复"] == 2, str(report["其中重复"]))
    check("校验失败=5", report["校验失败"] == 5, str(report["校验失败"]))
    check("超阈值", report["是否超阈值"] is True)
    check("异常比例≈53.8%", report["异常比例"] == "53.8%", report["异常比例"])
    check("失败原因分布含五类规则",
          report["失败原因分布"] == {"类型匹配": 3, "正则表达式": 1, "非空检查": 1, "取值范围": 1, "重复检查": 2},
          str(report["失败原因分布"]))
    check("message 不是笼统成功", "入管道 6 条" in message and "隔离 7 条" in message, message)

    output = read_ndjson(data_dir / "PIPE-117.ndjson")
    check("NDJSON 文件 6 行", len(output) == 6, str(len(output)))
    check("NDJSON 首行完成字段映射与类型转换",
          output[0] == {"title": "无线 蓝牙耳机", "price": 199.0, "publish_time": "2026-11-01"},
          json.dumps(output[0], ensure_ascii=False) if output else "空")
    check("NDJSON 主键唯一",
          len({(row["title"], row["publish_time"]) for row in output}) == len(output))

    quarantine = read_ndjson(data_dir / "PIPE-117.quarantine.ndjson")
    check("隔离区文件 7 行", len(quarantine) == 7, str(len(quarantine)))
    check("隔离行保留原始JSON、字段名和原因",
          all("原始JSON" in row and row["失败字段"] and row["失败原因"] for row in quarantine))
    empty_title = next(row for row in quarantine if row["原始JSON"]["price"] == "¥59")
    check("空标题行按非空检查隔离", empty_title["失败字段"] == "标题" and "非空" in empty_title["失败原因"],
          empty_title["失败原因"])
    dup_rows = [row for row in quarantine if "重复" in row["失败原因"]]
    check("重复行进入隔离区且说明主键", len(dup_rows) == 2 and "标题=" in dup_rows[0]["失败原因"])

    ledger = next(row for row in store.rows("resultcheck") if row["校验编号"] == "RESU-117")
    check("校验台账标记异常", ledger["abnormal"] is True)
    check("校验台账记录异常比例", ledger["最近异常比例"] == "53.8%", str(ledger.get("最近异常比例")))
    check("校验台账记录失败原因", "重复检查失败×2" in ledger["最近失败原因"], str(ledger.get("最近失败原因")))

    logs = store.rows("storagelog")[log_before:]
    check("采集日志新增一条", len(logs) == 1, str(len(logs)))
    log = logs[0]
    check("采集日志条数对账", log["请求总数"] == 13 and log["成功条数"] == 6 and log["失败条数"] == 7)
    check("采集日志与台账同步显示失败原因", log["失败原因"] == ledger["最近失败原因"])
    check("采集日志标记异常", log["abnormal"] is True)


def test_rerun_only_fills_missing() -> None:
    print("[test_rerun_only_fills_missing] 重复执行只补做缺失步骤")
    service, data_dir = fresh_service()
    first, _ = service.run_chain(task_code="TASK-117")
    assert first is not None
    log_before = len(store.rows("storagelog"))
    second, message = service.run_chain(task_code="TASK-117")
    assert second is not None
    check("第二次运行处理 0 条", second["本次处理"] == 0, str(second["本次处理"]))
    check("第二次运行跳过 13 条", second["跳过已处理"] == 13, str(second["跳过已处理"]))
    check("第二次运行入管道 0 条", second["入管道"] == 0)
    check("NDJSON 仍是 6 行", len(read_ndjson(data_dir / "PIPE-117.ndjson")) == 6)
    check("隔离区仍是 7 行", len(read_ndjson(data_dir / "PIPE-117.quarantine.ndjson")) == 7)
    check("无新增采集日志", len(store.rows("storagelog")) == log_before)

    # 模拟进程重启：同一数据目录新建服务实例，重跑仍然幂等
    restarted = QualityChainService(data_dir=data_dir)
    third, _ = restarted.run_chain(task_code="TASK-117")
    assert third is not None
    check("重启后重跑处理 0 条", third["本次处理"] == 0, str(third["本次处理"]))
    check("重启后 NDJSON 仍是 6 行", len(read_ndjson(data_dir / "PIPE-117.ndjson")) == 6)
    check("重启后隔离区仍是 7 行", len(read_ndjson(data_dir / "PIPE-117.quarantine.ndjson")) == 7)


def test_concurrent_runs_keep_single_key() -> None:
    print("[test_concurrent_runs_keep_single_key] 并发处理同一批记录，相同主键只留一条")
    service, data_dir = fresh_service()
    reports: list[dict] = []
    errors: list[str] = []

    def worker() -> None:
        try:
            report, message = service.run_chain(task_code="TASK-117")
            if report is not None:
                reports.append(report)
            else:
                errors.append(message)
        except Exception as exc:  # noqa: BLE001
            errors.append(repr(exc))

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    check("并发运行无异常", not errors, "; ".join(errors))
    check("4 个线程都拿到报告", len(reports) == 4, str(len(reports)))
    check("入管道合计=6", sum(report["入管道"] for report in reports) == 6,
          str([report["入管道"] for report in reports]))
    check("隔离合计=7", sum(report["隔离区新增"] for report in reports) == 7,
          str([report["隔离区新增"] for report in reports]))
    output = read_ndjson(data_dir / "PIPE-117.ndjson")
    keys = [(row["title"], row["publish_time"]) for row in output]
    check("NDJSON 6 行且主键唯一", len(output) == 6 and len(set(keys)) == 6, str(len(output)))


def test_resume_precleaned_record() -> None:
    print("[test_resume_precleaned_record] 历史已清洗记录只补做后续步骤")
    service, data_dir = fresh_service()
    report, _ = service.run_chain(task_code="TASK-117")
    assert report is not None
    record13 = next(row for row in service._raw_records if row["id"] == 13)
    check("历史记录保留原有清洗结果",
          record13["quality"]["clean"] == {"标题": "旧数据补做行", "价格": "49.9", "发布时间": "2026-10-31"},
          str(record13["quality"].get("clean")))
    check("历史记录补做后入管道", record13["quality"].get("pipelined") is True)
    output = read_ndjson(data_dir / "PIPE-117.ndjson")
    check("管道产出包含历史记录",
          any(row["title"] == "旧数据补做行" and row["price"] == 49.9 for row in output),
          json.dumps(output, ensure_ascii=False))
    per_record = {item["记录ID"]: item for item in report["逐条结果"]}
    check("逐条结果可追溯", per_record[13]["结果"] == "入管道" and per_record[3]["结果"] == "隔离")


def test_cleaning_and_types() -> None:
    print("[test_cleaning_and_types] 字符清洗正则与类型/范围检查生效")
    service, _ = fresh_service()
    report, _ = service.run_chain(task_code="TASK-117")
    assert report is not None
    record1 = service._raw_records[0]
    check("空白与全角空格被清洗", record1["quality"]["clean"]["标题"] == "无线 蓝牙耳机",
          record1["quality"]["clean"]["标题"])
    check("货币符号被管道清洗表达式去除", record1["quality"]["clean"]["价格"] == "199.00",
          record1["quality"]["clean"]["价格"])
    check("价格转换为数值", record1["quality"]["valid"]["价格"] == 199.0)
    record5 = service._raw_records[4]
    check("超范围价格被取值范围拦截",
          any(f["规则"] == "取值范围" for f in record5["quality"]["failures"]),
          str(record5["quality"].get("failures")))
    record7 = service._raw_records[6]
    rules7 = {f["规则"] for f in record7["quality"]["failures"]}
    check("非法日期同时触发类型与正则", rules7 == {"类型匹配", "正则表达式"}, str(rules7))


def test_missing_links_error() -> None:
    print("[test_missing_links_error] 配置缺环时报具体环节，不返回笼统成功")
    service, _ = fresh_service()
    report, message = service.run_chain(task_code="TASK-0001")
    check("占位任务缺解析规则", report is None and "解析规则" in message, message)
    report, message = service.run_chain(task_code="TASK-9999")
    check("任务不存在", report is None and "TASK-9999" in message, message)
    report, message = service.run_chain(pipeline_code="PIPE-117")
    check("按管道反查任务运行", report is not None and report["任务编号"] == "TASK-117", message)
    report, message = service.run_chain(pipeline_code="PIPE-0001")
    check("占位管道报数据来源无效", report is None and "数据来源" in message, message)


def main() -> int:
    test_full_chain()
    test_rerun_only_fills_missing()
    test_concurrent_runs_keep_single_key()
    test_resume_precleaned_record()
    test_cleaning_and_types()
    test_missing_links_error()
    print(f"\n合计 {PASS} 通过 / {FAIL} 失败")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
