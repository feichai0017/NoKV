import unittest
import importlib.metadata
import json
import urllib.error
from pathlib import Path
import sys
from unittest import mock

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


class FakeBridgeResponse:
    def __init__(self, body: dict) -> None:
        self.body = json.dumps(body).encode("utf-8")

    def __enter__(self) -> "FakeBridgeResponse":
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        pass

    def read(self) -> bytes:
        return self.body


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

    def test_tool_bridge_retries_connection_reset(self) -> None:
        attempts = 0

        def fake_urlopen(request, timeout):  # type: ignore[no-untyped-def]
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise urllib.error.URLError(
                    ConnectionResetError(54, "Connection reset by peer")
                )
            return FakeBridgeResponse({"status": "success", "content": {"ok": True}})

        with mock.patch.object(runner.urllib.request, "urlopen", side_effect=fake_urlopen):
            with mock.patch.object(runner.time, "sleep") as sleep:
                response = runner.post_tool_bridge(
                    "http://127.0.0.1:12345",
                    "run-1",
                    "stat",
                    {"path": "/yanex/runs"},
                    attempts=2,
                    retry_sleep_seconds=0,
                )

        self.assertEqual(response["content"], {"ok": True})
        self.assertEqual(attempts, 2)
        sleep.assert_called_once()

    def test_tool_bridge_error_message_includes_arguments(self) -> None:
        error = urllib.error.URLError(ConnectionResetError(54, "Connection reset by peer"))

        message = runner.tool_bridge_error_message(
            "stat",
            {"path": "/yanex/runs/02b8a944/artifacts/git_diff.patch"},
            error,
        )

        self.assertIn("Error running tool stat", message)
        self.assertIn("/yanex/runs/02b8a944/artifacts/git_diff.patch", message)
        self.assertIn("Connection reset by peer", message)


if __name__ == "__main__":
    unittest.main()
