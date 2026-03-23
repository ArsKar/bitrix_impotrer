import math
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import pandas as pd
import requests


TIMEOUT = 30
REQUEST_DELAY = 0.2

COL_FULL_NAME = "Название компании (полное)"
COL_SHORT_NAME = "Название компании (сокращенное)"
COL_OGRN = "ОГРН"
COL_INN = "ИНН"
COL_LEGAL_ADDRESS = "Юр. адрес"
COL_REGION = "Регион"
COL_REG_DATE = "Дата регистрации"
COL_CEO_NAME = "ФИО руководителя"
COL_CEO_POSITION = "Должность руководителя"
COL_OKVED_CODE = "Основной ОКВЭД (код)"
COL_OKVED_DESC = "Основной ОКВЭД (описание)"
COL_EMPLOYEES = "Количество сотрудников"
COL_PHONES = "Телефоны"
COL_SITES = "Сайты"
COL_EMAIL = "Email"
COL_REVENUE = "Выручка за последний предоставленный период"
COL_PROFIT = "Прибыль за последний предоставленный период"

COMMENT_FIELDS = [
    ("Название компании (сокращенное)", COL_SHORT_NAME),
    ("ОГРН", COL_OGRN),
    ("ИНН", COL_INN),
    ("Юр. адрес", COL_LEGAL_ADDRESS),
    ("Регион", COL_REGION),
    ("Дата регистрации", COL_REG_DATE),
    ("ФИО руководителя", COL_CEO_NAME),
    ("Должность руководителя", COL_CEO_POSITION),
    ("Основной ОКВЭД (код)", COL_OKVED_CODE),
    ("Основной ОКВЭД (описание)", COL_OKVED_DESC),
    ("Количество сотрудников", COL_EMPLOYEES),
    ("Телефоны", COL_PHONES),
    ("Сайты", COL_SITES),
    ("Email", COL_EMAIL),
    ("Выручка", COL_REVENUE),
    ("Прибыль", COL_PROFIT),
]


@dataclass
class ImportConfig:
    webhook_url: str
    excel_file: str
    entity_type: str
    contact_mode: str
    result_file: Optional[str] = None
    sheet_name: int | str = 0
    deal_category_id: Optional[int] = None
    deal_stage_id: str = ""
    lead_status_id: str = "NEW"
    check_duplicate_company_by_inn: bool = True
    check_duplicate_contact: bool = True


def api_call(webhook_url: str, method: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    url = webhook_url.rstrip("/") + f"/{method}.json"
    response = requests.post(url, json=payload or {}, timeout=TIMEOUT)
    response.raise_for_status()
    data = response.json()
    if "error" in data:
        raise RuntimeError(f"{data.get('error')}: {data.get('error_description')}")
    return data


def load_excel_preview(excel_file: str, sheet_name: int | str = 0, rows: int = 5) -> pd.DataFrame:
    return pd.read_excel(excel_file, sheet_name=sheet_name, nrows=rows)


def read_excel(excel_file: str, sheet_name: int | str = 0) -> pd.DataFrame:
    return pd.read_excel(excel_file, sheet_name=sheet_name)


def safe_str(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    return str(value).strip()


def safe_num_str(value: Any) -> str:
    text = safe_str(value)
    if text.endswith(".0"):
        return text[:-2]
    return text


def normalize_date(value: Any) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return ""
    dt = pd.to_datetime(value, errors="coerce")
    if pd.isna(dt):
        return ""
    return dt.strftime("%Y-%m-%d")


def split_multi(value: Any) -> List[str]:
    text = safe_str(value)
    if not text:
        return []

    parts = [text]
    for sep in [";", ",", "\n"]:
        next_parts: List[str] = []
        for part in parts:
            next_parts.extend(part.split(sep))
        parts = next_parts

    result: List[str] = []
    seen = set()
    for part in parts:
        item = part.strip()
        if not item:
            continue
        key = item.lower()
        if key in seen:
            continue
        seen.add(key)
        result.append(item)
    return result


def parse_phones(value: Any) -> List[Dict[str, str]]:
    return [{"VALUE": item, "VALUE_TYPE": "WORK"} for item in split_multi(value)]


def parse_emails(value: Any) -> List[Dict[str, str]]:
    return [{"VALUE": item, "VALUE_TYPE": "WORK"} for item in split_multi(value)]


def parse_websites(value: Any) -> List[Dict[str, str]]:
    items = []
    for site in split_multi(value):
        if not site.startswith(("http://", "https://")):
            site = "https://" + site
        items.append({"VALUE": site, "VALUE_TYPE": "WORK"})
    return items


def first_phone(value: Any) -> str:
    phones = split_multi(value)
    return phones[0] if phones else ""


def first_email(value: Any) -> str:
    emails = split_multi(value)
    return emails[0] if emails else ""


def split_person_name(full_name: str) -> Dict[str, str]:
    parts = full_name.split()
    if not parts:
        return {"NAME": "", "LAST_NAME": ""}
    if len(parts) == 1:
        return {"NAME": parts[0], "LAST_NAME": ""}
    return {"LAST_NAME": parts[0], "NAME": parts[1]}


def build_title(row: pd.Series, default_title: str) -> str:
    company_name = safe_str(row.get(COL_FULL_NAME))
    inn = safe_num_str(row.get(COL_INN))
    if company_name and inn:
        return f"{company_name} (ИНН {inn})"
    if company_name:
        return company_name
    return default_title


def build_comments(row: pd.Series, include_contacts: bool) -> str:
    lines = []
    for label, column in COMMENT_FIELDS:
        if not include_contacts and column in {COL_PHONES, COL_SITES, COL_EMAIL, COL_CEO_NAME, COL_CEO_POSITION}:
            continue
        value = normalize_date(row.get(column)) if column == COL_REG_DATE else safe_str(row.get(column))
        if value:
            lines.append(f"{label}: {value}")
    return "\n".join(lines)


def validate_columns(df: pd.DataFrame) -> None:
    required = {COL_FULL_NAME, COL_INN}
    missing = [column for column in required if column not in df.columns]
    if missing:
        raise RuntimeError(f"В файле не найдены обязательные колонки: {', '.join(missing)}")


def find_company_by_inn(webhook_url: str, inn: str) -> Optional[int]:
    if not inn:
        return None
    data = api_call(webhook_url, "crm.company.list", {
        "filter": {"TITLE": None},
        "select": ["ID", "TITLE"],
    })
    for item in data.get("result", []):
        if safe_num_str(item.get("UF_CRM_INN")) == inn:
            return int(item["ID"])
    return None


def find_contact(webhook_url: str, phone: str, email: str) -> Optional[int]:
    if phone:
        data = api_call(webhook_url, "crm.contact.list", {
            "filter": {"PHONE": phone},
            "select": ["ID", "NAME", "LAST_NAME"],
        })
        result = data.get("result", [])
        if result:
            return int(result[0]["ID"])

    if email:
        data = api_call(webhook_url, "crm.contact.list", {
            "filter": {"EMAIL": email},
            "select": ["ID", "NAME", "LAST_NAME"],
        })
        result = data.get("result", [])
        if result:
            return int(result[0]["ID"])

    return None


def build_company_fields(row: pd.Series) -> Dict[str, Any]:
    fields: Dict[str, Any] = {
        "TITLE": safe_str(row.get(COL_FULL_NAME)) or "Без названия",
    }

    phones = parse_phones(row.get(COL_PHONES))
    emails = parse_emails(row.get(COL_EMAIL))
    sites = parse_websites(row.get(COL_SITES))

    if phones:
        fields["PHONE"] = phones
    if emails:
        fields["EMAIL"] = emails
    if sites:
        fields["WEB"] = sites

    return fields


def create_company(webhook_url: str, row: pd.Series) -> int:
    data = api_call(webhook_url, "crm.company.add", {"fields": build_company_fields(row)})
    return int(data["result"])


def build_contact_fields(row: pd.Series, company_id: int) -> Dict[str, Any]:
    ceo = safe_str(row.get(COL_CEO_NAME))
    name_parts = split_person_name(ceo)
    fields: Dict[str, Any] = {
        "NAME": name_parts["NAME"] or ceo or "Контакт",
        "LAST_NAME": name_parts["LAST_NAME"],
        "COMPANY_ID": company_id,
    }

    phones = parse_phones(row.get(COL_PHONES))
    emails = parse_emails(row.get(COL_EMAIL))
    position = safe_str(row.get(COL_CEO_POSITION))

    if phones:
        fields["PHONE"] = phones
    if emails:
        fields["EMAIL"] = emails
    if position:
        fields["POST"] = position

    return fields


def create_contact(webhook_url: str, row: pd.Series, company_id: int) -> int:
    phone = first_phone(row.get(COL_PHONES))
    email = first_email(row.get(COL_EMAIL))
    existing_id = find_contact(webhook_url, phone, email)
    if existing_id:
        return existing_id
    data = api_call(webhook_url, "crm.contact.add", {"fields": build_contact_fields(row, company_id)})
    return int(data["result"])


def build_deal_fields(row: pd.Series, config: ImportConfig, company_id: Optional[int]) -> Dict[str, Any]:
    fields: Dict[str, Any] = {
        "TITLE": build_title(row, "Новая сделка"),
        "CATEGORY_ID": config.deal_category_id,
        "STAGE_ID": config.deal_stage_id,
        "COMMENTS": build_comments(row, include_contacts=(config.contact_mode == "comments")),
    }
    if company_id:
        fields["COMPANY_ID"] = company_id
    return fields


def create_deal(webhook_url: str, row: pd.Series, config: ImportConfig) -> Dict[str, Any]:
    company_id: Optional[int] = None
    contact_id: Optional[int] = None

    if config.contact_mode == "entities":
        company_id = create_company(webhook_url, row)
        contact_id = create_contact(webhook_url, row, company_id)

    data = api_call(webhook_url, "crm.deal.add", {
        "fields": build_deal_fields(row, config, company_id),
    })
    deal_id = int(data["result"])

    if contact_id:
        api_call(webhook_url, "crm.deal.contact.items.set", {
            "id": deal_id,
            "items": [{"CONTACT_ID": contact_id, "IS_PRIMARY": "Y"}],
        })

    return {
        "company_id": company_id or "",
        "contact_id": contact_id or "",
        "deal_id": deal_id,
    }


def build_lead_fields(row: pd.Series, config: ImportConfig) -> Dict[str, Any]:
    ceo = safe_str(row.get(COL_CEO_NAME))
    name_parts = split_person_name(ceo)

    fields: Dict[str, Any] = {
        "TITLE": build_title(row, "Новый лид"),
        "STATUS_ID": config.lead_status_id,
        "COMPANY_TITLE": safe_str(row.get(COL_FULL_NAME)) or safe_str(row.get(COL_SHORT_NAME)),
        "NAME": name_parts["NAME"] or ceo or "Контакт",
        "LAST_NAME": name_parts["LAST_NAME"],
        "POST": safe_str(row.get(COL_CEO_POSITION)),
        "ADDRESS": safe_str(row.get(COL_LEGAL_ADDRESS)),
        "COMMENTS": build_comments(row, include_contacts=(config.contact_mode == "comments")),
    }

    if config.contact_mode == "entities":
        phones = parse_phones(row.get(COL_PHONES))
        emails = parse_emails(row.get(COL_EMAIL))
        websites = parse_websites(row.get(COL_SITES))
        if phones:
            fields["PHONE"] = phones
        if emails:
            fields["EMAIL"] = emails
        if websites:
            fields["WEB"] = websites

    return fields


def create_lead(webhook_url: str, row: pd.Series, config: ImportConfig) -> Dict[str, Any]:
    data = api_call(webhook_url, "crm.lead.add", {
        "fields": build_lead_fields(row, config),
    })
    return {"lead_id": int(data["result"])}


def get_all_statuses(webhook_url: str) -> List[Dict[str, Any]]:
    data = api_call(webhook_url, "crm.status.list")
    return data.get("result", [])


def get_lead_statuses(webhook_url: str) -> List[Dict[str, Any]]:
    items = [item for item in get_all_statuses(webhook_url) if str(item.get("ENTITY_ID", "")) == "STATUS"]
    return sorted(items, key=lambda item: int(item.get("SORT", 0)))


def get_deal_categories(webhook_url: str) -> List[Dict[str, Any]]:
    data = api_call(webhook_url, "crm.category.list", {"entityTypeId": 2})
    result = data.get("result", {})
    if isinstance(result, dict):
        categories = result.get("categories", [])
    else:
        categories = result
    return sorted(categories, key=lambda item: int(item.get("sort", 0)))


def get_deal_stages(webhook_url: str) -> Dict[str, List[Dict[str, Any]]]:
    categories = get_deal_categories(webhook_url)
    statuses = get_all_statuses(webhook_url)
    category_names = {str(item.get("id")): str(item.get("name")) for item in categories}
    grouped: Dict[str, List[Dict[str, Any]]] = {}

    for status in statuses:
        entity_id = str(status.get("ENTITY_ID", ""))
        if entity_id == "DEAL_STAGE":
            category_id = "0"
        elif entity_id.startswith("DEAL_STAGE_"):
            category_id = entity_id.replace("DEAL_STAGE_", "", 1)
        else:
            continue
        item = dict(status)
        item["CATEGORY_NAME"] = category_names.get(category_id, "Общая воронка")
        grouped.setdefault(category_id, []).append(item)

    for category_id, items in grouped.items():
        grouped[category_id] = sorted(items, key=lambda item: int(item.get("SORT", 0)))
    return grouped


def format_lead_statuses(statuses: List[Dict[str, Any]]) -> str:
    lines = ["=== ЛИДЫ ==="]
    for item in statuses:
        lines.append(
            f"STATUS_ID={item.get('STATUS_ID')} | NAME={item.get('NAME')} | SORT={item.get('SORT')}"
        )
    return "\n".join(lines)


def format_deal_metadata(categories: List[Dict[str, Any]], stages: Dict[str, List[Dict[str, Any]]]) -> str:
    lines = ["=== ВОРОНКИ СДЕЛОК ==="]
    for category in categories:
        lines.append(
            f"CATEGORY_ID={category.get('id')} | NAME={category.get('name')} | SORT={category.get('sort')}"
        )

    lines.append("")
    lines.append("=== СТАДИИ ПО ВОРОНКАМ ===")
    for category_id in sorted(stages.keys(), key=lambda value: int(value) if value.isdigit() else value):
        items = stages[category_id]
        category_name = items[0].get("CATEGORY_NAME", "Общая воронка") if items else "Общая воронка"
        lines.append("")
        lines.append(f"[Воронка] CATEGORY_ID={category_id} | NAME={category_name}")
        for item in items:
            lines.append(
                f"  STATUS_ID={item.get('STATUS_ID')} | NAME={item.get('NAME')} | "
                f"ENTITY_ID={item.get('ENTITY_ID')} | SORT={item.get('SORT')} | "
                f"SEMANTICS={item.get('SEMANTICS')}"
            )
    return "\n".join(lines)


def default_result_file(excel_file: str, entity_type: str) -> str:
    source = Path(excel_file)
    suffix = "deals" if entity_type == "deal" else "leads"
    return str(source.with_name(f"{source.stem}_{suffix}_result.xlsx"))


def import_file(
    config: ImportConfig,
    log: Optional[Callable[[str], None]] = None,
    progress: Optional[Callable[[int, int], None]] = None,
) -> Dict[str, Any]:
    df = read_excel(config.excel_file, config.sheet_name)
    if df.empty:
        raise RuntimeError("Файл пустой.")
    validate_columns(df)

    result_file = config.result_file or default_result_file(config.excel_file, config.entity_type)
    results: List[Dict[str, Any]] = []
    total = len(df.index)

    for index, row in df.iterrows():
        row_num = index + 2
        try:
            if config.entity_type == "deal":
                item_result = create_deal(config.webhook_url, row, config)
            else:
                item_result = create_lead(config.webhook_url, row, config)

            result = {"row_number": row_num, "status": "OK", "error": ""}
            result.update(item_result)
            results.append(result)

            if log:
                ids = ", ".join(f"{key}={value}" for key, value in item_result.items() if value != "")
                log(f"[OK] Строка {row_num}: {ids}")
        except Exception as exc:
            result = {"row_number": row_num, "status": "ERROR", "error": str(exc)}
            if config.entity_type == "deal":
                result.update({"company_id": "", "contact_id": "", "deal_id": ""})
            else:
                result.update({"lead_id": ""})
            results.append(result)
            if log:
                log(f"[ERROR] Строка {row_num}: {exc}")

        if progress:
            progress(index + 1, total)
        time.sleep(REQUEST_DELAY)

    pd.DataFrame(results).to_excel(result_file, index=False)

    ok_count = sum(1 for item in results if item["status"] == "OK")
    error_count = sum(1 for item in results if item["status"] == "ERROR")
    summary = {
        "ok_count": ok_count,
        "error_count": error_count,
        "result_file": result_file,
        "total": total,
    }
    if log:
        log("")
        log("=== ГОТОВО ===")
        log(f"Успешно: {ok_count}")
        log(f"Ошибок: {error_count}")
        log(f"Отчет сохранен: {result_file}")
    return summary


def load_webhook_from_env(env_file: str = ".env") -> str:
    path = Path(env_file)
    if not path.exists():
        return ""
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith("BITRIX_URL_WEBWORKER="):
            return line.split("=", 1)[1].strip()
    return ""


def save_webhook_to_env(webhook_url: str, env_file: str = ".env") -> None:
    path = Path(env_file)
    lines: List[str] = []
    if path.exists():
        lines = path.read_text(encoding="utf-8").splitlines()

    updated = False
    for index, line in enumerate(lines):
        if line.startswith("BITRIX_URL_WEBWORKER="):
            lines[index] = f"BITRIX_URL_WEBWORKER={webhook_url}"
            updated = True
            break

    if not updated:
        lines.append(f"BITRIX_URL_WEBWORKER={webhook_url}")

    content = "\n".join(lines).strip() + "\n"
    path.write_text(content, encoding="utf-8")
