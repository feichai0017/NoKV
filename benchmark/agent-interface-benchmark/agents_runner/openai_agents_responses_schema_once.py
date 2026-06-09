#!/usr/bin/env python3
"""OpenAI Agents SDK schema-once runner for the Yanex benchmark."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Any


class RunnerContractError(RuntimeError):
    pass


@dataclass
class RecordedApiCall:
    request_id: str | None
    response_id: str | None
    model: str
    started_at_unix_ms: int
    completed_at_unix_ms: int
    previous_response_id: str | None
    sent_tool_schema: bool
    sent_initial_instructions: bool
    prompt_tokens: int | None
    completion_tokens: int | None
    total_tokens: int | None
    reasoning_tokens: int | None
    cached_prompt_tokens: int | None
    accepted_prediction_tokens: int | None
    rejected_prediction_tokens: int | None


@dataclass
class SchemaOnceResponsesModel:
    model: str
    endpoint: str
    api_key: str
    max_completion_tokens: int
    temperature: float | None
    response_format: dict[str, Any] | None
    api_calls: list[RecordedApiCall] = field(default_factory=list)

    async def get_response(
        self,
        system_instructions: str | None,
        input: str | list[Any],
        model_settings: Any,
        tools: list[Any],
        output_schema: Any,
        handoffs: list[Any],
        tracing: Any,
        *,
        previous_response_id: str | None,
        conversation_id: str | None,
        prompt: Any | None,
    ) -> Any:
        del model_settings, output_schema, handoffs, tracing, conversation_id, prompt
        request = self.build_request(system_instructions, input, tools, previous_response_id)
        raw, request_id, started, completed = await asyncio.to_thread(self.send_request, request)
        self.record_call(request, raw, request_id, started, completed)

        from agents.items import ModelResponse
        from agents.usage import Usage
        from openai.types.responses import Response

        response = Response.model_validate(raw)
        return ModelResponse(
            output=response.output,
            usage=usage_from_response(raw),
            response_id=response.id,
        )

    def stream_response(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError("streaming is not supported by the benchmark runner")

    def build_request(
        self,
        system_instructions: str | None,
        input_items: str | list[Any],
        tools: list[Any],
        previous_response_id: str | None,
    ) -> dict[str, Any]:
        first_request = previous_response_id is None
        normalized_input = normalize_input_items(input_items)
        if first_request:
            if system_instructions:
                normalized_input = [
                    {"role": "system", "content": system_instructions},
                    *normalized_input,
                ]
            request: dict[str, Any] = {
                "model": self.model,
                "input": normalized_input,
                "tools": serialize_tools(tools),
                "max_output_tokens": self.max_completion_tokens,
            }
            if self.temperature is not None:
                request["temperature"] = self.temperature
            if self.response_format is not None:
                request["text"] = {"format": self.response_format}
            return request

        request = {
            "previous_response_id": previous_response_id,
            "input": normalized_input,
        }
        assert_continuation_request_is_schema_once(request)
        return request

    def send_request(self, request: dict[str, Any]) -> tuple[dict[str, Any], str | None, int, int]:
        started = now_ms()
        body = json.dumps(request, separators=(",", ":")).encode("utf-8")
        http_request = urllib.request.Request(
            self.endpoint,
            data=body,
            method="POST",
            headers={
                "authorization": f"Bearer {self.api_key}",
                "content-type": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(http_request, timeout=120) as response:
                payload = response.read()
                request_id = response.headers.get("x-request-id")
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"OpenAI response HTTP {exc.code}: {detail}") from exc
        completed = now_ms()
        return json.loads(payload), request_id, started, completed

    def record_call(
        self,
        request: dict[str, Any],
        response: dict[str, Any],
        request_id: str | None,
        started: int,
        completed: int,
    ) -> None:
        usage = response.get("usage") or {}
        input_details = (
            usage.get("input_tokens_details")
            or usage.get("prompt_tokens_details")
            or {}
        )
        output_details = (
            usage.get("output_tokens_details")
            or usage.get("completion_tokens_details")
            or {}
        )
        self.api_calls.append(
            RecordedApiCall(
                request_id=request_id,
                response_id=response.get("id"),
                model=response.get("model") or self.model,
                started_at_unix_ms=started,
                completed_at_unix_ms=completed,
                previous_response_id=request.get("previous_response_id"),
                sent_tool_schema="tools" in request,
                sent_initial_instructions=input_contains_initial_instructions(request.get("input")),
                prompt_tokens=usage.get("input_tokens") or usage.get("prompt_tokens"),
                completion_tokens=usage.get("output_tokens") or usage.get("completion_tokens"),
                total_tokens=usage.get("total_tokens"),
                reasoning_tokens=output_details.get("reasoning_tokens"),
                cached_prompt_tokens=input_details.get("cached_tokens"),
                accepted_prediction_tokens=output_details.get("accepted_prediction_tokens"),
                rejected_prediction_tokens=output_details.get("rejected_prediction_tokens"),
            )
        )


def now_ms() -> int:
    return int(time.time() * 1000)


def normalize_input_items(input_items: str | list[Any]) -> list[Any]:
    if isinstance(input_items, str):
        return [{"role": "user", "content": input_items}]
    return [to_plain_json(item) for item in input_items]


def to_plain_json(value: Any) -> Any:
    if hasattr(value, "model_dump"):
        return value.model_dump(exclude_unset=True)
    if isinstance(value, dict):
        return {key: to_plain_json(item) for key, item in value.items()}
    if isinstance(value, list):
        return [to_plain_json(item) for item in value]
    return value


def serialize_tools(tools: list[Any]) -> list[dict[str, Any]]:
    serialized = []
    for tool in tools:
        serialized.append(
            {
                "type": "function",
                "name": getattr(tool, "name"),
                "description": getattr(tool, "description"),
                "parameters": getattr(tool, "params_json_schema"),
                "strict": getattr(tool, "strict_json_schema", True),
            }
        )
    return serialized


def assert_continuation_request_is_schema_once(request: dict[str, Any]) -> None:
    forbidden = {"tools", "instructions", "model", "max_output_tokens", "temperature", "text"}
    present = sorted(forbidden.intersection(request))
    if present:
        raise RunnerContractError(
            "schema-once continuation serialized repeated fields: " + ", ".join(present)
        )
    if "previous_response_id" not in request:
        raise RunnerContractError("schema-once continuation missing previous_response_id")
    if input_contains_initial_instructions(request.get("input")):
        raise RunnerContractError("schema-once continuation repeated initial instructions")


def input_contains_initial_instructions(input_items: Any) -> bool:
    if not isinstance(input_items, list):
        return False
    for item in input_items:
        if not isinstance(item, dict):
            continue
        role = item.get("role")
        if role in {"system", "developer"}:
            return True
    return False


def usage_from_response(raw: dict[str, Any]) -> Any:
    from agents.usage import Usage

    usage = raw.get("usage") or {}
    input_details = usage.get("input_tokens_details") or usage.get("prompt_tokens_details") or {}
    output_details = (
        usage.get("output_tokens_details") or usage.get("completion_tokens_details") or {}
    )
    try:
        from openai.types.responses.response_usage import (
            InputTokensDetails,
            OutputTokensDetails,
        )
    except Exception:
        return Usage(
            requests=1,
            input_tokens=usage.get("input_tokens") or usage.get("prompt_tokens") or 0,
            output_tokens=usage.get("output_tokens") or usage.get("completion_tokens") or 0,
            total_tokens=usage.get("total_tokens") or 0,
        )
    return Usage(
        requests=1,
        input_tokens=usage.get("input_tokens") or usage.get("prompt_tokens") or 0,
        output_tokens=usage.get("output_tokens") or usage.get("completion_tokens") or 0,
        total_tokens=usage.get("total_tokens") or 0,
        input_tokens_details=InputTokensDetails(
            cached_tokens=input_details.get("cached_tokens") or 0
        ),
        output_tokens_details=OutputTokensDetails(
            reasoning_tokens=output_details.get("reasoning_tokens") or 0
        ),
    )


def load_config(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def load_tools(config: dict[str, Any], harness_bin: str, arm: str) -> list[dict[str, Any]]:
    if os.getenv("YANEX_BENCH_AGENT_SDK_TEST_NO_SDK") == "1":
        return list(config["tools"])
    try:
        output = subprocess.check_output(
            [harness_bin, "tools", "--arm", arm],
            text=True,
            stderr=subprocess.PIPE,
        )
        return json.loads(output)
    except Exception:
        if config.get("tools"):
            return list(config["tools"])
        raise


def make_function_tools(tool_specs: list[dict[str, Any]], config: dict[str, Any]) -> list[Any]:
    from agents import FunctionTool

    tools = []
    for spec in tool_specs:
        name = spec["name"]

        async def invoke_tool(ctx: Any, args: str, tool_name: str = name) -> str:
            del ctx
            arguments = json.loads(args) if args else {}
            response = await asyncio.to_thread(
                post_tool_bridge,
                config["tool_bridge_url"],
                config["run_id"],
                tool_name,
                arguments,
            )
            if response.get("fatal_run_error"):
                raise RuntimeError(response.get("error") or f"tool {tool_name} failed")
            return json.dumps(response["content"], separators=(",", ":"))

        tools.append(
            FunctionTool(
                name=name,
                description=spec["description"],
                params_json_schema=spec["parameters"],
                on_invoke_tool=invoke_tool,
            )
        )
    return tools


def post_tool_bridge(
    tool_bridge_url: str,
    run_id: str,
    tool_name: str,
    arguments: dict[str, Any],
) -> dict[str, Any]:
    payload = json.dumps(
        {"run_id": run_id, "tool_name": tool_name, "arguments": arguments},
        separators=(",", ":"),
    ).encode("utf-8")
    request = urllib.request.Request(
        f"{tool_bridge_url}/invoke",
        data=payload,
        method="POST",
        headers={"content-type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=120) as response:
        return json.loads(response.read())


async def run_with_agents_sdk(config: dict[str, Any], harness_bin: str, arm: str) -> dict[str, Any]:
    try:
        from agents import Agent, ModelSettings, Runner, set_tracing_disabled
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "openai-agents>=0.7.0,<0.8.0 is required for "
            "openai_agents_responses_schema_once"
        ) from exc

    set_tracing_disabled(True)
    if os.getenv("YANEX_BENCH_AGENT_SDK_LIVE_PROBE") == "1":
        await asyncio.to_thread(run_live_probe, config)
    tool_specs = load_tools(config, harness_bin, arm)
    model = SchemaOnceResponsesModel(
        model=config["model"],
        endpoint=config["endpoint"],
        api_key=os.environ["OPENAI_API_KEY"],
        max_completion_tokens=config["max_completion_tokens"],
        temperature=config.get("temperature"),
        response_format=config.get("response_format"),
    )
    messages = config["messages"]
    instructions = "\n\n".join(
        message.get("content", "") for message in messages if message.get("role") == "system"
    )
    user_input = "\n\n".join(
        message.get("content", "") for message in messages if message.get("role") == "user"
    )
    agent = Agent(
        name="Yanex benchmark agent",
        instructions=instructions,
        model=model,
        tools=make_function_tools(tool_specs, config),
        model_settings=ModelSettings(),
    )
    try:
        result = await Runner.run(
            agent,
            input=user_input,
            max_turns=config["max_turns"],
            auto_previous_response_id=True,
        )
        final_output = result.final_output
        final_answer = parse_final_answer(final_output)
        run_error = None
    except Exception as exc:
        final_output = None
        final_answer = None
        run_error = str(exc)
    return {
        "final_answer": final_answer,
        "final_output": final_output,
        "run_error": run_error,
        "api_calls": [call.__dict__ for call in model.api_calls],
    }


def run_live_probe(config: dict[str, Any]) -> None:
    api_key = os.environ["OPENAI_API_KEY"]
    probe_tool = {
        "type": "function",
        "name": "schema_once_probe",
        "description": "Return a probe marker.",
        "parameters": {
            "type": "object",
            "properties": {},
            "additionalProperties": False,
        },
        "strict": True,
    }
    first_request = {
        "model": config["model"],
        "input": [
            {
                "role": "user",
                "content": "Call schema_once_probe once, then use its output to answer ok.",
            }
        ],
        "tools": [probe_tool],
        "tool_choice": "required",
        "max_output_tokens": 128,
    }
    first_response = send_probe_request(config["endpoint"], api_key, first_request)
    calls = response_tool_calls(first_response)
    if not calls:
        raise RunnerContractError("live schema-once probe did not receive a function call")
    second_request = {
        "previous_response_id": first_response["id"],
        "input": [
            {
                "type": "function_call_output",
                "call_id": calls[0]["call_id"],
                "output": json.dumps({"ok": True}, separators=(",", ":")),
            }
        ],
    }
    assert_continuation_request_is_schema_once(second_request)
    send_probe_request(config["endpoint"], api_key, second_request)


def send_probe_request(endpoint: str, api_key: str, request_body: dict[str, Any]) -> dict[str, Any]:
    body = json.dumps(request_body, separators=(",", ":")).encode("utf-8")
    request = urllib.request.Request(
        endpoint,
        data=body,
        method="POST",
        headers={
            "authorization": f"Bearer {api_key}",
            "content-type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            return json.loads(response.read())
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RunnerContractError(f"live schema-once probe failed HTTP {exc.code}: {detail}") from exc


async def run_without_agents_sdk(config: dict[str, Any]) -> dict[str, Any]:
    tool_specs = list(config["tools"])
    model = SchemaOnceResponsesModel(
        model=config["model"],
        endpoint=config["endpoint"],
        api_key=os.environ["OPENAI_API_KEY"],
        max_completion_tokens=config["max_completion_tokens"],
        temperature=config.get("temperature"),
        response_format=config.get("response_format"),
    )
    tools = [SimpleTool(spec) for spec in tool_specs]
    first_response, _, _, _ = await asyncio.to_thread(
        model.send_request,
        model.build_request(None, config["messages"], tools, None),
    )
    model.record_call(
        model.build_request(None, config["messages"], tools, None),
        first_response,
        None,
        now_ms(),
        now_ms(),
    )
    calls = response_tool_calls(first_response)
    previous_response_id = first_response["id"]
    if calls:
        tool_outputs = []
        for call in calls:
            bridge_response = await asyncio.to_thread(
                post_tool_bridge,
                config["tool_bridge_url"],
                config["run_id"],
                call["name"],
                json.loads(call["arguments"]),
            )
            if bridge_response.get("fatal_run_error"):
                return {
                    "final_answer": None,
                    "final_output": None,
                    "run_error": bridge_response.get("error"),
                    "api_calls": [record.__dict__ for record in model.api_calls],
                }
            tool_outputs.append(
                {
                    "type": "function_call_output",
                    "call_id": call["call_id"],
                    "output": json.dumps(bridge_response["content"], separators=(",", ":")),
                }
            )
        second_request = model.build_request(None, tool_outputs, tools, previous_response_id)
        second_response, _, second_started, second_completed = await asyncio.to_thread(
            model.send_request,
            second_request,
        )
        model.record_call(second_request, second_response, None, second_started, second_completed)
        final_output = response_final_text(second_response)
        return {
            "final_answer": parse_final_answer(final_output),
            "final_output": final_output,
            "run_error": None,
            "api_calls": [record.__dict__ for record in model.api_calls],
        }
    final_output = response_final_text(first_response)
    return {
        "final_answer": parse_final_answer(final_output),
        "final_output": final_output,
        "run_error": None,
        "api_calls": [record.__dict__ for record in model.api_calls],
    }


class SimpleTool:
    def __init__(self, spec: dict[str, Any]) -> None:
        self.name = spec["name"]
        self.description = spec["description"]
        self.params_json_schema = spec["parameters"]
        self.strict_json_schema = True


def response_tool_calls(response: dict[str, Any]) -> list[dict[str, str]]:
    calls = []
    for item in response.get("output") or []:
        if item.get("type") != "function_call":
            continue
        calls.append(
            {
                "call_id": item["call_id"],
                "name": item["name"],
                "arguments": item["arguments"],
            }
        )
    return calls


def response_final_text(response: dict[str, Any]) -> str | None:
    if response.get("output_text"):
        return response["output_text"]
    text = []
    for item in response.get("output") or []:
        if item.get("type") != "message":
            continue
        content = item.get("content")
        if isinstance(content, str):
            text.append(content)
        elif isinstance(content, list):
            for part in content:
                if part.get("type") == "output_text" and isinstance(part.get("text"), str):
                    text.append(part["text"])
    return "".join(text) or None


def parse_final_answer(final_output: Any) -> Any:
    if final_output is None:
        return None
    if not isinstance(final_output, str):
        return final_output
    return json.loads(final_output)


def run(args: argparse.Namespace) -> int:
    config = load_config(args.config)
    try:
        if os.getenv("YANEX_BENCH_AGENT_SDK_TEST_NO_SDK") == "1":
            result = asyncio.run(run_without_agents_sdk(config))
        else:
            result = asyncio.run(run_with_agents_sdk(config, args.harness_bin, args.arm))
    except Exception as exc:
        result = {
            "final_answer": None,
            "final_output": None,
            "run_error": str(exc),
            "api_calls": [],
        }
    print(json.dumps(result, separators=(",", ":")), flush=True)
    return 0


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    run_parser = subparsers.add_parser("run")
    run_parser.add_argument("--config", required=True)
    run_parser.add_argument("--harness-bin", required=True)
    run_parser.add_argument("--arm", required=True)
    parsed = parser.parse_args(argv)
    if parsed.command == "run":
        return run(parsed)
    raise AssertionError(parsed.command)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
