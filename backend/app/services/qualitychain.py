"""采集结果质量链路：把解析清洗、结果校验、异常隔离、去重入管道串成一条可重入的处理链。

跨模块贯通方式：
- 解析规则（parserule）提供「输出字段映射」与「字符清洗正则」；
- 预处理管道（pipeline）提供「字段映射」「清洗表达式」「去重字段」「输出格式」「异常行策略」；
- 结果校验（resultcheck）提供「非空检查」「类型匹配」「正则表达式」「取值范围」「重复检查」「记录数阈值」；
- 每轮运行把异常比例与失败原因写回结果校验台账，并生成一条采集日志（storagelog），
  超过阈值时两处同步标记异常。

可靠性与兼容性：
- 每条原始记录携带质量步骤标记（cleaned / validated / quarantined / pipelined），
  重新执行时只补做缺失步骤，兼容历史未处理数据；
- 去重主键集合在模块级锁内维护，并发处理同一批记录时相同主键只保留一条；
- 通过校验且完成去重的记录追加写入 UTF-8 NDJSON 管道文件；
- 失败行保留原始 JSON、字段名与失败原因，进入异常隔离区；
- 处理状态持久化到 data/qualitychain/state.json，进程重启后重跑仍然幂等。
"""
from __future__ import annotations

import json
import os
import re
import threading
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

from app.store import store

DEFAULT_DATA_DIR = Path(
    os.environ.get("QUALITYCHAIN_DATA_DIR")
    or Path(__file__).resolve().parents[2] / "data" / "qualitychain"
)

# ---------------------------------------------------------------------------
# 配置解析：把各模块里的中文字符串配置解析成可执行规则
# ---------------------------------------------------------------------------


def _parse_mapping(text: Any) -> dict[str, str]:
    """解析字段映射：支持 JSON 对象或 `a=b, c=d` 形式；样例占位文本解析为空映射。"""
    raw = str(text or "").strip()
    if not raw:
        return {}
    try:
        data = json.loads(raw)
        if isinstance(data, dict):
            return {
                str(key).strip(): str(value).strip()
                for key, value in data.items()
                if str(key).strip() and str(value).strip()
            }
    except (ValueError, TypeError):
        pass
    mapping: dict[str, str] = {}
    for part in re.split(r"[;；,，\n]+", raw):
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        key, value = key.strip(), value.strip()
        if key and value:
            mapping[key] = value
    return mapping


def _parse_sed(expr: str) -> tuple[re.Pattern[str], str, int] | None:
    """解析一条 sed 风格清洗规则 `s/模式/替换/flags`，g 表示全部替换。"""
    expr = expr.strip()
    if not expr.startswith("s/"):
        return None
    parts = expr[2:].split("/")
    if len(parts) < 2:
        return None
    pattern, repl = parts[0], parts[1]
    flags = parts[2] if len(parts) > 2 else ""
    if not pattern:
        return None
    try:
        compiled = re.compile(pattern)
    except re.error:
        return None
    return compiled, repl, 0 if "g" in flags else 1


def _parse_clean_rules(text: Any) -> list[tuple[re.Pattern[str], str, int]]:
    """解析全局字符清洗正则：`s/a/b/g; s/c/d/` 分号分隔。"""
    rules: list[tuple[re.Pattern[str], str, int]] = []
    for seg in re.split(r"[;；]", str(text or "")):
        parsed = _parse_sed(seg)
        if parsed:
            rules.append(parsed)
    return rules


def _parse_field_clean(text: Any) -> dict[str, list[tuple[re.Pattern[str], str, int]]]:
    """解析按字段清洗表达式：`价格: s/[¥￥,]//g; 标题: s/x/y/`；不带字段名的规则进 `*`。"""
    rules: dict[str, list[tuple[re.Pattern[str], str, int]]] = {}
    for seg in re.split(r"[;；]", str(text or "")):
        seg = seg.strip()
        if not seg:
            continue
        field = "*"
        expr = seg
        parts = re.split(r"[:：]", seg, 1)
        if len(parts) == 2 and parts[1].strip().startswith("s/"):
            field, expr = parts[0].strip(), parts[1]
        parsed = _parse_sed(expr)
        if parsed:
            rules.setdefault(field, []).append(parsed)
    return rules


def _parse_field_specs(text: Any) -> dict[str, str]:
    """解析 `字段: 值; 字段: 值` 形式的按字段配置（类型匹配 / 正则表达式 / 取值范围）。"""
    specs: dict[str, str] = {}
    for seg in re.split(r"[;；]", str(text or "")):
        seg = seg.strip()
        if not seg:
            continue
        parts = re.split(r"[:：]", seg, 1)
        if len(parts) == 2 and parts[0].strip() and parts[1].strip():
            specs[parts[0].strip()] = parts[1].strip()
    return specs


def _parse_field_list(text: Any) -> list[str]:
    """解析字段清单：`标题，发布时间`。"""
    return [part for part in re.split(r"[,，、;；\s]+", str(text or "")) if part]


def _parse_key_fields(text: Any) -> list[str]:
    """解析去重主键：`标题+发布时间`。"""
    return [part.strip() for part in re.split(r"[+＋]", str(text or "")) if part.strip()]


def _parse_range(spec: str) -> tuple[float | None, float | None] | None:
    """解析数值范围 `0~1000000`，允许留空一端。"""
    match = re.match(r"^\s*(-?\d+(?:\.\d+)?)?\s*[~～]\s*(-?\d+(?:\.\d+)?)?\s*$", spec)
    if not match:
        return None
    low = float(match.group(1)) if match.group(1) else None
    high = float(match.group(2)) if match.group(2) else None
    return low, high


def _parse_threshold(text: Any) -> float | None:
    """解析异常比例阈值：`30%` 或 `30`，统一返回百分数；解析不出视为未设阈值。"""
    match = re.search(r"-?\d+(?:\.\d+)?", str(text or ""))
    if not match:
        return None
    value = float(match.group(0))
    return value if value > 1 else value * 100


def _coerce_value(value: Any, type_name: str) -> tuple[bool, Any]:
    """按类型匹配规则校验并转换单个值，返回 (是否通过, 转换后的值)。"""
    text = str(value).strip()
    kind = type_name.strip().lower()
    if kind in ("string", "str", "text", "文本", "字符串"):
        return True, text
    if kind in ("number", "float", "numeric", "decimal", "数值", "数字"):
        try:
            return True, float(text)
        except ValueError:
            return False, None
    if kind in ("int", "integer", "long", "整数"):
        if re.fullmatch(r"-?\d+", text):
            return True, int(text)
        return False, None
    if kind in ("date", "日期"):
        try:
            datetime.strptime(text, "%Y-%m-%d")
            return True, text
        except ValueError:
            return False, None
    if kind in ("datetime", "timestamp", "时间", "日期时间"):
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
            try:
                datetime.strptime(text, fmt)
                return True, text
            except ValueError:
                continue
        return False, None
    if kind in ("bool", "boolean", "布尔"):
        if text.lower() in ("true", "false", "0", "1"):
            return True, text.lower() in ("true", "1")
        return False, None
    return True, text  # 未识别的类型名不拦截，只按字符串通过


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# ---------------------------------------------------------------------------
# 质量链路服务
# ---------------------------------------------------------------------------


class QualityChainService:
    """解析 → 清洗 → 校验 → 隔离/去重 → 入管道 的完整质量链路。"""

    def __init__(self, data_dir: Path | None = None) -> None:
        self._lock = threading.RLock()
        self._data_dir = Path(data_dir) if data_dir else DEFAULT_DATA_DIR
        self._raw_records: list[dict[str, Any]] = []
        self._quarantine: list[dict[str, Any]] = []
        self._pipeline_records: list[dict[str, Any]] = []
        self._runs: list[dict[str, Any]] = []
        self._dedup_keys: dict[str, set[str]] = {}
        self._run_seq = 0
        self._quar_seq = 0
        self._seed_raw_records()
        self._restore_state()

    # ------------------------------------------------------------------
    # 示例原始数据：TASK-117 经 PARS-117 抽取出的原始记录（含各类坏行与重复行）
    # ------------------------------------------------------------------

    def _seed_raw_records(self) -> None:
        samples: list[dict[str, Any]] = [
            # 正常行：含多余空白、全角空格、货币符号，验证字符清洗
            {"title": "  无线　蓝牙耳机  ", "price": "¥199.00", "pub_time": "2026-11-01"},
            {"title": "机械键盘", "price": "￥299", "pub_time": "2026-11-02"},
            {"title": "", "price": "¥59", "pub_time": "2026-11-01"},                      # 标题为空
            {"title": "智能音箱", "price": "面议", "pub_time": "2026-11-03"},               # 价格非数值
            {"title": "降噪耳机", "price": "¥1299999", "pub_time": "2026-11-03"},           # 价格超出取值范围
            {"title": "无线 蓝牙耳机", "price": "¥199.00", "pub_time": "2026-11-01"},       # 与第 1 条主键重复
            {"title": "便携显示器", "price": "¥899", "pub_time": "2026年11月2日"},           # 发布时间格式非法
            {"title": "人体工学椅", "price": "¥1299", "pub_time": "2026-11-04"},
            {"title": " 机械键盘 ", "price": "¥299", "pub_time": "2026-11-02"},             # 与第 2 条主键重复
            {"title": "桌面支架", "price": "", "pub_time": "2026-11-05"},                   # 价格为空
            {"title": "蓝牙鼠标", "price": "¥79.9", "pub_time": "2026-11-05"},
            {"title": "USB-C 扩展坞", "price": "¥159", "pub_time": "2026-11-06"},
        ]
        for index, sample in enumerate(samples, start=1):
            self._raw_records.append({
                "id": index,
                "任务编号": "TASK-117",
                "规则编号": "PARS-117",
                "原始JSON": sample,
                "quality": {},
            })
        # 历史遗留记录：升级前已完成字段清洗，重跑时只补做校验、去重与入管道
        self._raw_records.append({
            "id": len(samples) + 1,
            "任务编号": "TASK-117",
            "规则编号": "PARS-117",
            "原始JSON": {"title": "旧数据补做行", "price": "49.9", "pub_time": "2026-10-31"},
            "quality": {
                "cleaned": True,
                "clean": {"标题": "旧数据补做行", "价格": "49.9", "发布时间": "2026-10-31"},
            },
        })

    # ------------------------------------------------------------------
    # 状态持久化：进程重启后重跑仍只补做缺失步骤，且不重复写入管道
    # ------------------------------------------------------------------

    @property
    def _state_file(self) -> Path:
        return self._data_dir / "state.json"

    def _restore_state(self) -> None:
        state_path = self._state_file
        if not state_path.exists():
            return
        try:
            state = json.loads(state_path.read_text(encoding="utf-8"))
        except (ValueError, OSError):
            return
        self._run_seq = int(state.get("run_seq", 0))
        self._quar_seq = int(state.get("quar_seq", 0))
        self._dedup_keys = {
            str(pipe): set(keys) for pipe, keys in (state.get("dedup") or {}).items()
        }
        saved = state.get("records") or {}
        for record in self._raw_records:
            key = f"{record['任务编号']}:{record['id']}"
            if key in saved and isinstance(saved[key], dict):
                record["quality"] = saved[key]
        self._quarantine = list(state.get("quarantine") or [])
        self._pipeline_records = list(state.get("pipeline_records") or [])
        self._runs = list(state.get("runs") or [])

    def _save_state(self) -> None:
        self._data_dir.mkdir(parents=True, exist_ok=True)
        state = {
            "run_seq": self._run_seq,
            "quar_seq": self._quar_seq,
            "dedup": {pipe: sorted(keys) for pipe, keys in self._dedup_keys.items()},
            "records": {
                f"{record['任务编号']}:{record['id']}": record["quality"]
                for record in self._raw_records
                if record.get("quality")
            },
            "quarantine": self._quarantine,
            "pipeline_records": self._pipeline_records,
            "runs": self._runs[-50:],
        }
        tmp_path = self._state_file.with_suffix(".tmp")
        tmp_path.write_text(json.dumps(state, ensure_ascii=False, indent=1), encoding="utf-8")
        os.replace(tmp_path, self._state_file)

    def _append_ndjson(self, path: Path, rows: list[dict[str, Any]]) -> None:
        if not rows:
            return
        self._data_dir.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, ensure_ascii=False) + "\n")

    # ------------------------------------------------------------------
    # 配置解析：跨模块把 parserule / pipeline / resultcheck 的配置拼成执行计划
    # ------------------------------------------------------------------

    def _find_by_code(self, module: str, field: str, code: str) -> dict[str, Any] | None:
        for row in store.rows(module):
            if str(row.get(field, "")).strip() == code:
                return row
        return None

    def _build_plan(
        self,
        parse_rule: dict[str, Any],
        check_rule: dict[str, Any],
        pipeline: dict[str, Any],
    ) -> tuple[dict[str, Any], list[str]]:
        warnings: list[str] = []
        for label, row in (("解析规则", parse_rule), ("校验规则", check_rule), ("处理管道", pipeline)):
            if row.get("status") != "已启用":
                warnings.append(f"{label}「{row.get('规则编号') or row.get('校验编号') or row.get('管道编号')}」当前状态为「{row.get('status')}」，未处于已启用状态")

        range_specs: dict[str, tuple[float | None, float | None]] = {}
        for field, spec in _parse_field_specs(check_rule.get("取值范围")).items():
            parsed = _parse_range(spec)
            if parsed is None:
                warnings.append(f"取值范围配置「{field}: {spec}」无法解析，已跳过该规则")
            else:
                range_specs[field] = parsed

        regex_specs: dict[str, re.Pattern[str]] = {}
        for field, pattern in _parse_field_specs(check_rule.get("正则表达式")).items():
            try:
                regex_specs[field] = re.compile(pattern)
            except re.error:
                warnings.append(f"正则表达式配置「{field}: {pattern}」无法编译，已跳过该规则")

        dedup_fields = _parse_key_fields(check_rule.get("重复检查")) or _parse_key_fields(pipeline.get("去重字段"))
        if not dedup_fields:
            warnings.append("重复检查与去重字段均为空，本批次不做去重")

        output_format = str(pipeline.get("输出格式") or "").upper()
        if "NDJSON" not in output_format:
            warnings.append(f"输出格式「{pipeline.get('输出格式')}」暂按 NDJSON(UTF-8) 处理")

        plan = {
            "field_mapping": _parse_mapping(parse_rule.get("输出字段映射")),
            "char_clean_rules": _parse_clean_rules(parse_rule.get("字符清洗正则")),
            "field_clean_rules": _parse_field_clean(pipeline.get("清洗表达式")),
            "output_mapping": _parse_mapping(pipeline.get("字段映射")),
            "nonempty_fields": _parse_field_list(check_rule.get("非空检查")),
            "type_specs": _parse_field_specs(check_rule.get("类型匹配")),
            "regex_specs": regex_specs,
            "range_specs": range_specs,
            "dedup_fields": dedup_fields,
            "threshold": _parse_threshold(check_rule.get("记录数阈值")),
        }
        return plan, warnings

    # ------------------------------------------------------------------
    # 单条记录的处理步骤
    # ------------------------------------------------------------------

    def _clean_record(self, record: dict[str, Any], plan: dict[str, Any]) -> None:
        """字段映射 + 字符清洗正则 + 管道清洗表达式，产出清洗后的字段集。"""
        quality = record["quality"]
        if quality.get("cleaned"):
            return
        clean: dict[str, Any] = {}
        for raw_key, value in (record.get("原始JSON") or {}).items():
            field = plan["field_mapping"].get(raw_key, raw_key)
            if isinstance(value, str):
                for pattern, repl, count in plan["char_clean_rules"]:
                    value = pattern.sub(repl, value, count=count)
            clean[field] = value
        field_rules = plan["field_clean_rules"]
        for field, value in list(clean.items()):
            if not isinstance(value, str):
                continue
            for pattern, repl, count in field_rules.get("*", []) + field_rules.get(field, []):
                value = pattern.sub(repl, value, count=count)
            clean[field] = value
        quality["clean"] = clean
        quality["cleaned"] = True

    def _validate_record(self, record: dict[str, Any], plan: dict[str, Any]) -> list[dict[str, str]]:
        """非空 → 类型 → 正则 → 取值范围，收集全部失败原因；通过时产出类型化字段集。"""
        quality = record["quality"]
        if quality.get("validated"):
            return []
        clean: dict[str, Any] = quality.get("clean") or {}
        failures: list[dict[str, str]] = []

        for field in plan["nonempty_fields"]:
            if not str(clean.get(field) or "").strip():
                failures.append({"规则": "非空检查", "字段": field, "原因": f"字段「{field}」为空或缺失"})

        coerced: dict[str, Any] = {}
        for field, type_name in plan["type_specs"].items():
            if field not in clean:
                continue
            ok, value = _coerce_value(clean[field], type_name)
            if ok:
                coerced[field] = value
            else:
                failures.append({
                    "规则": "类型匹配",
                    "字段": field,
                    "原因": f"字段「{field}」的值「{clean[field]}」不是 {type_name} 类型",
                })

        for field, pattern in plan["regex_specs"].items():
            if field not in clean:
                continue
            if not pattern.search(str(clean[field])):
                failures.append({
                    "规则": "正则表达式",
                    "字段": field,
                    "原因": f"字段「{field}」的值「{clean[field]}」不匹配 {pattern.pattern}",
                })

        for field, (low, high) in plan["range_specs"].items():
            target = coerced.get(field)
            if not isinstance(target, (int, float)):
                try:
                    target = float(str(clean.get(field) or "").strip())
                except ValueError:
                    continue  # 非数值的失败由类型匹配规则负责，不重复记账
            if low is not None and target < low:
                failures.append({"规则": "取值范围", "字段": field, "原因": f"字段「{field}」的值 {target} 小于下限 {low}"})
            elif high is not None and target > high:
                failures.append({"规则": "取值范围", "字段": field, "原因": f"字段「{field}」的值 {target} 超出上限 {high}"})

        if failures:
            quality["failures"] = failures
            return failures
        valid = dict(clean)
        valid.update(coerced)
        quality["valid"] = valid
        quality["validated"] = True
        return []

    def _dedup_key(self, valid: dict[str, Any], dedup_fields: list[str]) -> str:
        return "|".join(f"{field}={valid.get(field, '')}" for field in dedup_fields)

    def _quarantine_record(
        self,
        record: dict[str, Any],
        failures: list[dict[str, str]],
        ctx: dict[str, Any],
    ) -> dict[str, Any]:
        """失败行保留原始 JSON、字段名与原因，进入异常隔离区。"""
        self._quar_seq += 1
        fields = list(dict.fromkeys(failure["字段"] for failure in failures))
        entry = {
            "id": self._quar_seq,
            "隔离编号": f"QUAR-{self._quar_seq:04d}",
            "任务编号": ctx["任务编号"],
            "规则编号": ctx["规则编号"],
            "校验编号": ctx["校验编号"],
            "管道编号": ctx["管道编号"],
            "原始JSON": record.get("原始JSON"),
            "失败字段": "、".join(fields),
            "失败原因": "；".join(f"{failure['规则']}：{failure['原因']}" for failure in failures),
            "失败明细": failures,
            "隔离时间": _now(),
            "运行编号": ctx["运行编号"],
        }
        record["quality"]["quarantined"] = True
        record["quality"]["failures"] = failures
        self._quarantine.append(entry)
        return entry

    # ------------------------------------------------------------------
    # 运行质量链路
    # ------------------------------------------------------------------

    def run_chain(
        self,
        *,
        task_code: str | None = None,
        pipeline_code: str | None = None,
        rule_code: str | None = None,
        check_code: str | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        """按任务跑一次完整质量链路；返回 (运行报告, 说明)，配置缺失时报告为 None。"""
        with self._lock:
            resolved, error = self._resolve_links(
                task_code=task_code, pipeline_code=pipeline_code,
                rule_code=rule_code, check_code=check_code,
            )
            if error:
                return None, error
            task, parse_rule, check_rule, pipeline = resolved
            ctx = {
                "任务编号": task["任务编号"],
                "规则编号": parse_rule["规则编号"],
                "校验编号": check_rule["校验编号"],
                "管道编号": pipeline["管道编号"],
            }
            plan, warnings = self._build_plan(parse_rule, check_rule, pipeline)

            self._run_seq += 1
            ctx["运行编号"] = f"RUN-{self._run_seq:04d}"
            started = _now()

            records = [row for row in self._raw_records if row["任务编号"] == ctx["任务编号"]]
            per_record: list[dict[str, Any]] = []
            cleaned_now = validated_now = 0
            pipelined_rows: list[dict[str, Any]] = []
            quarantined_rows: list[dict[str, Any]] = []
            dedup_keys = self._dedup_keys.setdefault(ctx["管道编号"], set())

            for record in records:
                quality = record["quality"]
                if quality.get("pipelined") or quality.get("quarantined"):
                    per_record.append({"记录ID": record["id"], "结果": "跳过", "原因": "质量步骤已完成，无需补做"})
                    continue
                was_cleaned = bool(quality.get("cleaned"))
                self._clean_record(record, plan)
                if not was_cleaned:
                    cleaned_now += 1
                if not quality.get("validated"):
                    validated_now += 1
                failures = self._validate_record(record, plan)
                if failures:
                    entry = self._quarantine_record(record, failures, ctx)
                    quarantined_rows.append(entry)
                    per_record.append({
                        "记录ID": record["id"], "结果": "隔离",
                        "原因": entry["失败原因"],
                    })
                    continue
                valid = quality["valid"]
                key = self._dedup_key(valid, plan["dedup_fields"]) if plan["dedup_fields"] else ""
                if key and key in dedup_keys:
                    failures = [{
                        "规则": "重复检查",
                        "字段": "+".join(plan["dedup_fields"]),
                        "原因": f"重复记录：主键「{key}」已存在，仅保留一条",
                    }]
                    entry = self._quarantine_record(record, failures, ctx)
                    quarantined_rows.append(entry)
                    per_record.append({"记录ID": record["id"], "结果": "隔离", "原因": failures[0]["原因"]})
                    continue
                if key:
                    dedup_keys.add(key)
                output = {plan["output_mapping"].get(field, field): value for field, value in valid.items()}
                pipelined_rows.append(output)
                quality["dedup_key"] = key
                quality["pipelined"] = True
                self._pipeline_records.append({
                    "id": len(self._pipeline_records) + 1,
                    "管道编号": ctx["管道编号"],
                    "任务编号": ctx["任务编号"],
                    "去重键": key,
                    "记录": output,
                    "写入时间": _now(),
                    "运行编号": ctx["运行编号"],
                })
                per_record.append({"记录ID": record["id"], "结果": "入管道", "原因": ""})

            # 本次处理 = 未跳过的记录数（含只补做部分步骤的历史记录）
            processed = sum(1 for item in per_record if item["结果"] != "跳过")
            skipped = len(per_record) - processed
            quarantined = len(quarantined_rows)
            pipelined = len(pipelined_rows)
            duplicated = sum(
                1 for row in quarantined_rows
                if any(failure["规则"] == "重复检查" for failure in row["失败明细"])
            )

            output_file = self._data_dir / f"{ctx['管道编号']}.ndjson"
            quarantine_file = self._data_dir / f"{ctx['管道编号']}.quarantine.ndjson"
            self._append_ndjson(output_file, pipelined_rows)
            self._append_ndjson(quarantine_file, quarantined_rows)

            ratio = quarantined / processed * 100 if processed else 0.0
            threshold = plan["threshold"]
            exceeded = threshold is not None and ratio > threshold and processed > 0
            breakdown = Counter(
                failure["规则"] for row in quarantined_rows for failure in row["失败明细"]
            )
            reason_text = "；".join(f"{name}失败×{count}" for name, count in breakdown.items()) or "无"

            log_code = ""
            if processed > 0:
                log_code = self._sync_ledger_and_log(
                    ctx, started=started, processed=processed, pipelined=pipelined,
                    quarantined=quarantined, ratio=ratio, exceeded=exceeded,
                    reason_text=reason_text,
                )

            finished = _now()
            report = {
                "运行编号": ctx["运行编号"],
                **ctx,
                "开始时间": started,
                "结束时间": finished,
                "记录总数": len(records),
                "本次处理": processed,
                "跳过已处理": skipped,
                "清洗补做": cleaned_now,
                "校验执行": validated_now,
                "校验通过": pipelined + duplicated,
                "校验失败": quarantined - duplicated,
                "其中重复": duplicated,
                "入管道": pipelined,
                "隔离区新增": quarantined,
                "隔离区总量": len(self._quarantine),
                "异常比例": f"{ratio:.1f}%",
                "异常阈值": f"{threshold:g}%" if threshold is not None else "未设置",
                "是否超阈值": exceeded,
                "失败原因分布": dict(breakdown),
                "失败原因": reason_text,
                "输出文件": str(output_file),
                "隔离区文件": str(quarantine_file),
                "日志编号": log_code,
                "配置警告": warnings,
                "逐条结果": per_record,
            }
            self._runs.append({key: report[key] for key in (
                "运行编号", "任务编号", "规则编号", "校验编号", "管道编号",
                "开始时间", "结束时间", "本次处理", "入管道", "隔离区新增",
                "异常比例", "是否超阈值", "失败原因",
            )})
            self._save_state()

            message = (
                f"质量链路运行完成：共 {len(records)} 条记录，本次处理 {processed} 条"
                f"（跳过已处理 {skipped} 条），入管道 {pipelined} 条，隔离 {quarantined} 条"
                f"（{reason_text}），异常比例 {ratio:.1f}%"
            )
            if threshold is not None:
                message += f"，阈值 {threshold:g}%，{'已超阈值，台账与采集日志已标记异常' if exceeded else '未超阈值'}"
            return report, message

    def _resolve_links(
        self,
        *,
        task_code: str | None,
        pipeline_code: str | None,
        rule_code: str | None,
        check_code: str | None,
    ) -> tuple[tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]] | None, str]:
        """把任务、解析规则、校验规则、处理管道四个模块的配置串起来，缺哪环报哪环。"""
        pipeline = None
        if pipeline_code:
            pipeline = self._find_by_code("pipeline", "管道编号", pipeline_code)
            if pipeline is None:
                return None, f"处理管道 {pipeline_code} 不存在或已归档"
        if not task_code and pipeline is not None:
            task_code = str(pipeline.get("数据来源") or "").strip()
        if not task_code:
            return None, "请提供任务编号或管道编号，才能定位要处理的数据批次"
        task = self._find_by_code("taskcrawl", "任务编号", task_code)
        if task is None:
            if pipeline is not None:
                return None, f"管道 {pipeline['管道编号']} 的数据来源「{task_code}」不是已登记的采集任务编号"
            return None, f"采集任务 {task_code} 不存在或已归档"

        rule_code = rule_code or str(task.get("规则编号") or "").strip()
        parse_rule = self._find_by_code("parserule", "规则编号", rule_code) if rule_code else None
        if parse_rule is None:
            return None, f"采集任务 {task_code} 关联的解析规则「{rule_code or '未填写'}」不存在，无法执行字段映射与字符清洗"

        check_code = check_code or str(task.get("校验编号") or "").strip()
        check_rule = self._find_by_code("resultcheck", "校验编号", check_code) if check_code else None
        if check_rule is None:
            return None, f"采集任务 {task_code} 未关联有效的校验规则（校验编号「{check_code or '未填写'}」），无法执行非空、类型、正则、范围与重复检查"

        if pipeline is None:
            pipeline_code = pipeline_code or str(task.get("管道编号") or "").strip()
            if pipeline_code:
                pipeline = self._find_by_code("pipeline", "管道编号", pipeline_code)
            if pipeline is None:
                pipeline = self._find_by_code("pipeline", "数据来源", task_code)
        if pipeline is None:
            return None, f"采集任务 {task_code} 未关联有效的处理管道，通过校验的记录无处写入"
        return (task, parse_rule, check_rule, pipeline), ""

    def _sync_ledger_and_log(
        self,
        ctx: dict[str, Any],
        *,
        started: str,
        processed: int,
        pipelined: int,
        quarantined: int,
        ratio: float,
        exceeded: bool,
        reason_text: str,
    ) -> str:
        """结果校验台账与采集日志同步记账：异常比例超阈值时两处都标记异常并写明失败原因。"""
        check_row = self._find_by_code("resultcheck", "校验编号", ctx["校验编号"])
        if check_row is not None:
            check_row["最近执行时间"] = started
            check_row["最近运行编号"] = ctx["运行编号"]
            check_row["最近异常比例"] = f"{ratio:.1f}%"
            check_row["最近失败原因"] = reason_text
            check_row["abnormal"] = exceeded

        log_rows = store.rows("storagelog")
        log_code = f"STOR-{9000 + self._run_seq}"
        log_rows.append({
            "id": max((int(row.get("id", 0)) for row in log_rows), default=0) + 1,
            "日志编号": log_code,
            "关联任务": ctx["任务编号"],
            "开始时间": started,
            "结束时间": _now(),
            "请求总数": processed,
            "成功条数": pipelined,
            "失败条数": quarantined,
            "平均响应毫秒": "—",
            "失败原因": reason_text,
            "异常比例": f"{ratio:.1f}%",
            "status": "已记录",
            "pending": False,
            "abnormal": exceeded,
        })
        return log_code

    # ------------------------------------------------------------------
    # 查询接口
    # ------------------------------------------------------------------

    def list_raw(self, task_code: str | None, page: int, size: int) -> tuple[list[dict[str, Any]], int]:
        rows = [row for row in self._raw_records if not task_code or row["任务编号"] == task_code]
        items = [{
            "id": row["id"],
            "任务编号": row["任务编号"],
            "规则编号": row["规则编号"],
            "原始JSON": row["原始JSON"],
            "质量步骤": {
                "已清洗": bool(row["quality"].get("cleaned")),
                "已通过校验": bool(row["quality"].get("validated")),
                "已隔离": bool(row["quality"].get("quarantined")),
                "已入管道": bool(row["quality"].get("pipelined")),
            },
            "失败明细": row["quality"].get("failures") or [],
        } for row in rows]
        start = max(page - 1, 0) * size
        return items[start:start + size], len(items)

    def list_quarantine(
        self,
        task_code: str | None,
        pipeline_code: str | None,
        page: int,
        size: int,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = self._quarantine
        if task_code:
            rows = [row for row in rows if row["任务编号"] == task_code]
        if pipeline_code:
            rows = [row for row in rows if row["管道编号"] == pipeline_code]
        start = max(page - 1, 0) * size
        return rows[start:start + size], len(rows)

    def list_output(self, pipeline_code: str | None, page: int, size: int) -> tuple[list[dict[str, Any]], int]:
        rows = self._pipeline_records
        if pipeline_code:
            rows = [row for row in rows if row["管道编号"] == pipeline_code]
        start = max(page - 1, 0) * size
        return rows[start:start + size], len(rows)

    def list_runs(self, page: int, size: int) -> tuple[list[dict[str, Any]], int]:
        rows = list(reversed(self._runs))
        start = max(page - 1, 0) * size
        return rows[start:start + size], len(rows)


qualitychain = QualityChainService()
