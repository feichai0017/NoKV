import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
import openai_agents_responses_schema_once as runner


class OpenAiAgentsVersionTest(unittest.TestCase):
    def test_accepts_only_openai_agents_0_7_series(self) -> None:
        self.assertTrue(runner.openai_agents_version_supported("0.7.0"))
        self.assertTrue(runner.openai_agents_version_supported("0.7.9"))
        self.assertFalse(runner.openai_agents_version_supported("0.6.9"))
        self.assertFalse(runner.openai_agents_version_supported("0.8.0"))
        self.assertFalse(runner.openai_agents_version_supported("1.0.0"))

    def test_requirement_text_matches_requirements_file(self) -> None:
        requirements = Path(__file__).resolve().parent.joinpath("requirements.txt").read_text()

        self.assertIn(runner.OPENAI_AGENTS_REQUIREMENT, requirements)


if __name__ == "__main__":
    unittest.main()
