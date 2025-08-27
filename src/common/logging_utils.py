# src/common/logging_utils.py
import sys

def _supports_unicode() -> bool:
    enc = (sys.stdout.encoding or "").lower()
    return "utf" in enc

if _supports_unicode():
    I = "ℹ️ "
    OK = "✅ "
    W = "⚠️ "
    E = "❌ "
else:
    I = "[i] "
    OK = "[ok] "
    W = "[!] "
    E = "[x] "

def info(msg: str): print(f"{I}{msg}")
def ok(msg: str):   print(f"{OK}{msg}")
def warn(msg: str): print(f"{W}{msg}")
def err(msg: str):  print(f"{E}{msg}")