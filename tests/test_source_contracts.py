from __future__ import annotations

import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MODULES = ROOT / "extension" / "CommonModules"


class SourceContractTests(unittest.TestCase):
    def test_bsl_lines_fit_project_limit(self):
        violations = []
        for path in MODULES.rglob("*.bsl"):
            for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
                if len(line) > 120:
                    violations.append(f"{path.relative_to(ROOT)}:{number}:{len(line)}")
        self.assertEqual([], violations)

    def test_bsl_blocks_are_balanced(self):
        for path in MODULES.rglob("*.bsl"):
            text = path.read_text(encoding="utf-8")
            starts = len(re.findall(r"^\s*(?:Процедура|Функция)\b", text, re.MULTILINE))
            ends = len(re.findall(r"^\s*Конец(?:Процедуры|Функции)\b", text, re.MULTILINE))
            self.assertEqual(starts, ends, path.relative_to(ROOT))
            self.assertEqual(text.count("#Область"), text.count("#КонецОбласти"), path.relative_to(ROOT))

    def test_consumer_persists_before_commit(self):
        path = MODULES / "KafkaИнтеграцияКонсьюмер" / "Ext" / "Module.bsl"
        text = path.read_text(encoding="utf-8")
        self.assertLess(text.index("ДобавитьЕслиОтсутствует"), text.index("Транспорт.Подтвердить"))

    def test_public_source_contains_no_binaries_or_processor_dump(self):
        suffixes = {path.suffix.lower() for path in ROOT.rglob("*") if path.is_file()}
        self.assertTrue({".epf", ".erf", ".dll", ".xml", ".bin"}.isdisjoint(suffixes))

    def test_logs_do_not_use_exception_details(self):
        journal = MODULES / "KafkaИнтеграцияБезопасныйЖурнал" / "Ext" / "Module.bsl"
        text = journal.read_text(encoding="utf-8")
        self.assertNotIn("ОписаниеОшибки", text)
        self.assertNotIn("ИнформацияОбОшибке", text)
        self.assertNotIn("ТекстСообщения", text)


if __name__ == "__main__":
    unittest.main()
