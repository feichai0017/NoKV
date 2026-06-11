import asyncio
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

    def test_url_validator_rejects_non_http_schemes(self) -> None:
        runner.assert_http_url(
            "https://api.openai.test/v1/responses",
            field_name="endpoint",
            allow_http=False,
        )
        runner.assert_http_url(
            "http://127.0.0.1:12345",
            field_name="tool_bridge_url",
            allow_http=True,
        )
        with self.assertRaises(runner.RunnerContractError):
            runner.assert_http_url(
                "http://api.openai.test/v1/responses",
                field_name="endpoint",
                allow_http=False,
            )
        with self.assertRaises(runner.RunnerContractError):
            runner.assert_http_url(
                "file:///tmp/token",
                field_name="tool_bridge_url",
                allow_http=True,
            )

    def test_no_sdk_path_records_real_request_metadata(self) -> None:
        sent_requests = []

        def fake_send_request(model, request):  # type: ignore[no-untyped-def]
            sent_requests.append(request)
            if "previous_response_id" not in request:
                return (
                    {
                        "id": "resp-1",
                        "model": "unit-model",
                        "output": [
                            {
                                "type": "function_call",
                                "call_id": "call-1",
                                "name": "stat",
                                "arguments": json.dumps({"path": "/runs"}),
                            }
                        ],
                        "usage": {"input_tokens": 7, "output_tokens": 3, "total_tokens": 10},
                    },
                    "req-1",
                    111,
                    222,
                )
            self.assertEqual(request["previous_response_id"], "resp-1")
            return (
                {
                    "id": "resp-2",
                    "model": "unit-model",
                    "output_text": json.dumps({"answer": "ok"}),
                    "usage": {"input_tokens": 5, "output_tokens": 2, "total_tokens": 7},
                },
                "req-2",
                333,
                444,
            )

        config = {
            "model": "unit-model",
            "endpoint": "https://example.test/v1/responses",
            "max_completion_tokens": 128,
            "messages": [{"role": "user", "content": "inspect /runs"}],
            "tools": [
                {
                    "name": "stat",
                    "description": "Stat a path.",
                    "parameters": {
                        "type": "object",
                        "properties": {"path": {"type": "string"}},
                        "required": ["path"],
                        "additionalProperties": False,
                    },
                }
            ],
            "tool_bridge_url": "http://127.0.0.1:12345",
            "run_id": "run-1",
        }

        with mock.patch.dict(runner.os.environ, {"OPENAI_API_KEY": "unit-key"}):
            with mock.patch.object(
                runner.SchemaOnceResponsesModel,
                "send_request",
                autospec=True,
                side_effect=fake_send_request,
            ):
                with mock.patch.object(
                    runner,
                    "post_tool_bridge",
                    return_value={"content": {"path": "/runs"}},
                ):
                    result = asyncio.run(runner.run_without_agents_sdk(config))

        self.assertEqual(result["final_answer"], {"answer": "ok"})
        self.assertEqual(len(sent_requests), 2)
        self.assertEqual(result["api_calls"][0]["request_id"], "req-1")
        self.assertEqual(result["api_calls"][0]["started_at_unix_ms"], 111)
        self.assertEqual(result["api_calls"][0]["completed_at_unix_ms"], 222)
        self.assertEqual(result["api_calls"][1]["request_id"], "req-2")
        self.assertEqual(result["api_calls"][1]["started_at_unix_ms"], 333)
        self.assertEqual(result["api_calls"][1]["completed_at_unix_ms"], 444)


if __name__ == "__main__":
    unittest.main()
