import json
import pandas as pd
from typing import Any, Dict
from .utils import format_key


def parse_nested_dict(consolidated: Dict[str, Any], entry: Dict[str, Any], prefix: str):
    """
    Recursively flatten a nested dict into the consolidated row with prefixed keys.
    """
    for k, v in entry.items():
        key = format_key(k)
        new_prefix = f"{prefix}_{key}"
        if isinstance(v, dict):
            parse_nested_dict(consolidated, v, new_prefix)
        else:
            consolidated[new_prefix] = v


def process_record(transposed_df: pd.DataFrame, record_content: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build a flat dict from the transposed DataFrame and original JSON metadata.
    """
    consolidated = {
        "BODY": json.dumps(record_content),
        "DSN": record_content.get("dsn"),
        "EVENT_ID": record_content.get("eventid"),
        "VIN": record_content.get("eventHeader", {}).get("emitterid"),
        "EMITTER_TYPE": record_content.get("eventHeader", {}).get("emitterType"),
        "EVENT_LEDGER_EVENT_TYPE": record_content.get("eventHeader", {}).get("eventLedgerEventType"),
        "EVENT_TYPE": record_content.get("eventHeader", {}).get("eventtype"),
        "OCCURRED": record_content.get("eventHeader", {}).get("occurred"),
    }
    key_counter: Dict[str, int] = {}

    for i in range(transposed_df.shape[1]):
        pgn = transposed_df.iloc[3, i]

        # Standard SPN rows
        spn = transposed_df.iloc[0, i]
        if pd.notnull(spn):
            spn = int(spn)
            consolidated[f"PGN_{pgn}_SPN_{spn}_TIME"] = transposed_df.iloc[1, i]
            consolidated[f"PGN_{pgn}_SPN_{spn}_VALUE"] = transposed_df.iloc[2, i]

        # DATA column (may be JSON string, dict, or list)
        data_cell = transposed_df.iloc[4, i]
        if isinstance(data_cell, str):
            try:
                data_cell = json.loads(data_cell)
            except json.JSONDecodeError:
                data_cell = []
        if isinstance(data_cell, dict):
            parse_nested_dict(consolidated, data_cell, f"DATA_PGN_{pgn}")
        elif isinstance(data_cell, list):
            for entry in data_cell:
                if isinstance(entry, dict):
                    parse_nested_dict(consolidated, entry, f"DATA_PGN_{pgn}")
                elif isinstance(entry, list):
                    for sub in entry:
                        if isinstance(sub, dict):
                            parse_nested_dict(consolidated, sub, f"DATA_PGN_{pgn}")

        # LAMPS column
        lamps_cell = transposed_df.iloc[5, i]
        if isinstance(lamps_cell, str):
            try:
                lamps_cell = json.loads(lamps_cell)
            except json.JSONDecodeError:
                lamps_cell = []
        if isinstance(lamps_cell, dict):
            lamps_cell = [lamps_cell]
        if isinstance(lamps_cell, list):
            for lamp in lamps_cell:
                if isinstance(lamp, dict):
                    spn_l = lamp.get("spn")
                    time_l = lamp.get("time")
                    val_l = lamp.get("value")
                    consolidated[f"LAMP_PGN_{pgn}_SPN_{spn_l}_TIME"] = time_l
                    consolidated[f"LAMP_PGN_{pgn}_SPN_{spn_l}_VALUE"] = val_l

    return consolidated