import unittest
import importlib.metadata
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
import openai_agents_responses_schema_once as runner


class FakeTool:
    name = "unit_tool"
    description = "Unit test tool."
    params_json_schema = {
        "type": "object",
        "properties": {"path": {"type": "string"}},
        "required": ["path"],
        "additionalProperties": False,
    }
    strict_json_schema = True


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

    def test_schema_once_model_implements_agents_model_interface(self) -> None:
        try:
            version = importlib.metadata.version("openai-agents")
        except importlib.metadata.PackageNotFoundError:
            self.skipTest("openai-agents is not installed")
        if not runner.openai_agents_version_supported(version):
            self.skipTest(f"openai-agents=={version} is outside the supported range")

        from agents.models.interface import Model

        model = runner.SchemaOnceResponsesModel(
            model="unit-model",
            endpoint="https://example.test/v1/responses",
            api_key="unit-key",
            max_completion_tokens=128,
            temperature=None,
            response_format=None,
        )

        self.assertIsInstance(model, Model)

    def test_continuation_request_keeps_model_and_tools_available(self) -> None:
        model = runner.SchemaOnceResponsesModel(
            model="unit-model",
            endpoint="https://example.test/v1/responses",
            api_key="unit-key",
            max_completion_tokens=128,
            temperature=1.0,
            response_format={"type": "json_object"},
        )

        request = model.build_request(
            system_instructions=None,
            input_items=[
                {
                    "type": "function_call_output",
                    "call_id": "call-1",
                    "output": "{}",
                }
            ],
            tools=[FakeTool()],
            previous_response_id="resp-1",
        )

        self.assertEqual(request["model"], "unit-model")
        self.assertEqual(request["previous_response_id"], "resp-1")
        self.assertEqual(request["tools"][0]["name"], "unit_tool")
        self.assertNotIn("text", request)
        self.assertNotIn("temperature", request)
        self.assertNotIn("max_output_tokens", request)


if __name__ == "__main__":
    unittest.main()
