from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from linkedin_cli.write import store
from test_content_harvest import PUBLIC_POST_HTML


class LLMProviderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "state.sqlite"
        self.artifacts_dir = Path(self.tempdir.name) / "artifacts"
        self.db_patcher = patch.object(store, "DB_PATH", self.db_path)
        self.artifacts_patcher = patch.object(store, "ARTIFACTS_DIR", self.artifacts_dir)
        self.db_patcher.start()
        self.artifacts_patcher.start()
        store.init_db()

        from linkedin_cli import content, llm_providers

        self.content = content
        self.llm_providers = llm_providers
        self.content.init_content_db()

    def tearDown(self) -> None:
        self.db_patcher.stop()
        self.artifacts_patcher.stop()
        self.tempdir.cleanup()

    def _seed_posts(self) -> None:
        for index, (title, reactions, comments) in enumerate(
            [
                ("AI workflow bottlenecks are where automation wins.", 260, 34),
                ("Most AI workflow advice is too generic to ship.", 210, 26),
            ],
            start=1,
        ):
            record = self.content.extract_post_record(
                url=f"https://www.linkedin.com/posts/claude-mackenzie_provider-{index}",
                html=PUBLIC_POST_HTML,
                source_query='site:linkedin.com/posts "ai workflow"',
            )
            record["industries"] = ["ai"]
            record["owned_by_me"] = True
            record["title"] = title
            record["hook"] = title
            record["text"] = (
                f"{title}\n\n"
                "We mapped the handoff, removed the break, and measured the cycle-time drop."
            )
            record["reaction_count"] = reactions
            record["comment_count"] = comments
            record["word_count"] = len(record["text"].split())
            self.content.upsert_post(record)
        self.content.train_outcome_model(name="default", min_samples=2, scope="owned", industry="ai", topics=["workflow"])

    def test_provider_config_round_trip_uses_local_config(self) -> None:
        saved = self.llm_providers.save_provider_config(
            provider_name="cerebras",
            api_key="secret-token",
            model="gpt-oss-120b",
        )
        loaded = self.llm_providers.load_provider_config("cerebras")

        self.assertEqual(saved["provider_name"], "cerebras")
        self.assertEqual(loaded["provider_name"], "cerebras")
        self.assertEqual(loaded["model"], "gpt-oss-120b")
        self.assertEqual(loaded["base_url"], "https://api.cerebras.ai/v1")
        self.assertEqual(loaded["api_key"], "secret-token")

    def test_cloud_generator_creates_ranked_candidates(self) -> None:
        self._seed_posts()
        self.llm_providers.save_provider_config(
            provider_name="cerebras",
            api_key="secret-token",
            model="gpt-oss-120b",
        )

        with patch("linkedin_cli.llm_providers.requests.post") as post_mock:
            post_mock.return_value.status_code = 200
            post_mock.return_value.json.return_value = {
                "choices": [
                    {
                        "message": {
                            "content": """
{
  "candidates": [
    {"goal": "engagement", "text": "AI workflow orchestration is still too brittle for operators. We fixed one handoff and measured the cycle-time drop. If you want the pattern, say pattern."},
    {"goal": "authority", "text": "Most teams do not have an AI model problem. They have a workflow reliability problem. We removed one approval break and turned the process into an operating system."}
  ]
}
"""
                        }
                    }
                ]
            }
            created = self.content.create_drafts(
                prompt="AI workflow orchestration is still too brittle for operators.",
                industry="ai",
                topics=["workflow"],
                model="local-hash-v1",
                candidate_goals=["engagement", "authority"],
                candidate_count=2,
                generator="cerebras",
            )

        self.assertEqual(len(created["candidates"]), 2)
        self.assertEqual(created["candidates"][0]["generator"]["source"], "cerebras:gpt-oss-120b")
        self.assertIn(created["candidates"][0]["goal"], {"engagement", "authority"})
        self.assertGreater(created["candidates"][0]["score"]["predicted_outcome_score"], 0)

    def test_auto_generator_prefers_provider_when_configured(self) -> None:
        self._seed_posts()
        self.llm_providers.save_provider_config(
            provider_name="cerebras",
            api_key="secret-token",
            model="gpt-oss-120b",
        )

        with patch("linkedin_cli.llm_providers.requests.post") as post_mock:
            post_mock.return_value.status_code = 200
            post_mock.return_value.json.return_value = {
                "choices": [
                    {
                        "message": {
                            "content": json.dumps(
                                {
                                    "candidates": [
                                        {
                                            "goal": "authority",
                                            "text": "Most teams do not have a model problem. They have a handoff problem. The fastest win is usually removing one approval break and proving the hours saved.",
                                        }
                                    ]
                                }
                            )
                        }
                    }
                ]
            }
            created = self.content.create_drafts(
                prompt="AI workflow orchestration is still too brittle for operators.",
                industry="ai",
                topics=["workflow"],
                model="local-hash-v1",
                candidate_goals=["authority"],
                candidate_count=1,
                generator="auto",
            )

        self.assertEqual(created["generator"], "cerebras")
        self.assertEqual(post_mock.call_count, 1)
        self.assertEqual(created["candidates"][0]["generator"]["source"], "cerebras:gpt-oss-120b")

    def test_auto_generator_falls_back_to_heuristic_when_provider_rejects_everything(self) -> None:
        self._seed_posts()
        self.llm_providers.save_provider_config(
            provider_name="cerebras",
            api_key="secret-token",
            model="gpt-oss-120b",
        )

        with patch("linkedin_cli.llm_providers.requests.post") as post_mock:
            post_mock.return_value.status_code = 200
            post_mock.return_value.json.return_value = {
                "choices": [
                    {
                        "message": {
                            "content": json.dumps(
                                {
                                    "candidates": [
                                        {
                                            "goal": "authority",
                                            "text": "When we tried to stitch together a large-language-model summarizer with our internal ticketing system, the routing layer failed first.",
                                        }
                                    ]
                                }
                            )
                        }
                    }
                ]
            }
            created = self.content.create_drafts(
                prompt="Create a LinkedIn post about why AI workflow automation breaks at the handoff.",
                industry="ai",
                topics=["workflow"],
                model="local-hash-v1",
                candidate_goals=["authority"],
                candidate_count=1,
                generator="auto",
            )

        self.assertEqual(created["generator_requested"], "auto")
        self.assertEqual(created["generator"], "heuristic")
        self.assertEqual(post_mock.call_count, 3)
        self.assertEqual(created["candidates"][0]["generator"]["source"], "playbook-heuristic-v1")
        self.assertEqual(created["candidates"][0]["generator"]["fallback_from"], "auto")

    def test_cloud_generator_rejects_fabricated_claims_and_retries(self) -> None:
        self._seed_posts()
        self.llm_providers.save_provider_config(
            provider_name="cerebras",
            api_key="secret-token",
            model="gpt-oss-120b",
        )

        responses = [
            {
                "choices": [
                    {
                        "message": {
                            "content": """
{
  "candidates": [
    {"goal": "engagement", "text": "We helped a Fortune 500 retailer cut failure rates by 68% and recover $1.2M in 4 weeks."},
    {"goal": "authority", "text": "Our client, a neobank, lifted conversion 42% after we rebuilt onboarding handoffs."}
  ]
}
"""
                        }
                    }
                ]
            },
            {
                "choices": [
                    {
                        "message": {
                            "content": """
{
  "candidates": [
    {"goal": "engagement", "text": "AI workflow orchestration breaks when teams treat handoffs like an implementation detail. The fastest win is usually removing one brittle approval break and measuring the before-and-after."},
    {"goal": "authority", "text": "Most AI workflow failures are orchestration failures, not model failures. Map the handoff, remove the break, and prove the result with evidence you can actually stand behind."}
  ]
}
"""
                        }
                    }
                ]
            },
        ]

        with patch("linkedin_cli.llm_providers.requests.post") as post_mock:
            post_mock.return_value.status_code = 200
            post_mock.return_value.json.side_effect = responses
            created = self.content.create_drafts(
                prompt="AI workflow orchestration is still too brittle for operators.",
                industry="ai",
                topics=["workflow"],
                model="local-hash-v1",
                candidate_goals=["engagement", "authority"],
                candidate_count=2,
                generator="cerebras",
            )

        self.assertEqual(len(created["candidates"]), 2)
        self.assertEqual(post_mock.call_count, 2)
        texts = [candidate["text"] for candidate in created["candidates"]]
        self.assertTrue(all("Fortune 500" not in text for text in texts))
        self.assertTrue(all("$1.2M" not in text for text in texts))
        self.assertTrue(all("Our client" not in text for text in texts))

    def test_cloud_generator_rejects_near_copying_exemplar_text_and_retries(self) -> None:
        self._seed_posts()
        self.llm_providers.save_provider_config(
            provider_name="cerebras",
            api_key="secret-token",
            model="gpt-oss-120b",
        )

        copied = (
            "AI workflow bottlenecks are where automation wins.\n\n"
            "We mapped the handoff, removed the break, and measured the cycle-time drop."
        )
        responses = [
            {
                "choices": [
                    {
                        "message": {
                            "content": json.dumps(
                                {
                                    "candidates": [
                                        {"goal": "engagement", "text": copied},
                                        {"goal": "authority", "text": copied},
                                    ]
                                }
                            )
                        }
                    }
                ]
            },
            {
                "choices": [
                    {
                        "message": {
                            "content": json.dumps(
                                {
                                    "candidates": [
                                        {
                                            "goal": "engagement",
                                            "text": "Most AI workflow rollouts do not fail in the model layer. They fail in the handoff nobody owns. The fastest win is tightening one routing break and proving the hours saved.",
                                        },
                                        {
                                            "goal": "authority",
                                            "text": "The operating problem in AI workflow automation is not clever prompting. It is whether the workflow survives retries, edge cases, and a real week of usage.",
                                        },
                                    ]
                                }
                            )
                        }
                    }
                ]
            },
        ]

        with patch("linkedin_cli.llm_providers.requests.post") as post_mock:
            post_mock.return_value.status_code = 200
            post_mock.return_value.json.side_effect = responses
            created = self.content.create_drafts(
                prompt="Create a LinkedIn post about why AI workflow automation breaks at the handoff.",
                industry="ai",
                topics=["workflow"],
                model="local-hash-v1",
                candidate_goals=["engagement", "authority"],
                candidate_count=2,
                generator="cerebras",
            )

        self.assertEqual(post_mock.call_count, 2)
        texts = [candidate["text"] for candidate in created["candidates"]]
        self.assertTrue(all(copied not in text for text in texts))

    def test_truth_validator_flags_unsupported_case_studies_and_metrics(self) -> None:
        issues = self.llm_providers.validate_candidate_truthfulness(
            text="We helped a Fortune 500 retailer cut failure rates by 68% and recover $1.2M in 4 weeks.",
            prompt="AI workflow orchestration is still too brittle for operators.",
        )

        self.assertTrue(issues)
        self.assertTrue(any("client" in issue or "case-study" in issue for issue in issues))
        self.assertTrue(any("numeric" in issue for issue in issues))

        built_issues = self.llm_providers.validate_candidate_truthfulness(
            text="When we built a customer-support bot for a mid-size SaaS firm, the routing layer failed first.",
            prompt="AI workflow orchestration is still too brittle for operators.",
        )
        self.assertTrue(any("unsupported first-person" in issue for issue in built_issues))

        wired_issues = self.llm_providers.validate_candidate_truthfulness(
            text="When we tried to stitch together a large-language-model summarizer with our internal ticketing system, the downstream service started throwing schema errors.",
            prompt="AI workflow orchestration is still too brittle for operators.",
        )
        self.assertTrue(any("case-study" in issue or "unsupported first-person" in issue for issue in wired_issues))

        anecdote_issues = self.llm_providers.validate_candidate_truthfulness(
            text="When a mid-size SaaS team tried to stitch together three LLM calls in one pipeline, the team spent an afternoon rewriting retry logic.",
            prompt="AI workflow orchestration is still too brittle for operators.",
        )
        self.assertTrue(any("case-study" in issue for issue in anecdote_issues))
        authority_issues = self.llm_providers.validate_candidate_truthfulness(
            text="Across three enterprise teams I've consulted, the same orchestration failures kept resurfacing.",
            prompt="AI workflow orchestration is still too brittle for operators.",
        )
        self.assertTrue(any("case-study" in issue for issue in authority_issues))

    def test_candidate_request_messages_use_latent_constraints_not_raw_example_hooks(self) -> None:
        messages = self.llm_providers._candidate_request_messages(
            prompt="Create a LinkedIn post about why AI workflow automation breaks at the handoff.",
            industry="ai",
            topics=["workflow"],
            candidate_goals=["engagement", "authority"],
            candidate_count=2,
            playbook={
                "hook_templates": [{"hook_type": "statement", "template": "{specific claim about topic}. Here is what happened when we tested it."}],
                "rewrite_rules": ["Add one metric, result, case, or concrete observation before the CTA."],
                "winning_structures": [{"structure": "list"}],
                "winning_topics": [{"name": "workflow"}],
                "learned_signals": {"top_positive": [{"feature": "proof"}]},
            },
            exemplars=[
                {
                    "hook": "Anthropic released how they're using Claude for Growth Marketing 🔥 It can 10x your output!",
                    "structure": "how_to",
                    "outcome_score": 23.57,
                    "relevance_score": 1.0,
                    "text": "Anthropic released how they're using Claude for Growth Marketing 🔥 It can 10x your output!",
                }
            ],
            brief={"audience": "engineering leaders", "objective": "drive demos"},
        )

        user_payload = json.loads(messages[1]["content"])
        self.assertIn("example_constraints", user_payload)
        self.assertNotIn("top_examples", user_payload)
        serialized = json.dumps(user_payload, ensure_ascii=False)
        self.assertNotIn("Anthropic released how they're using Claude", serialized)

    def test_provider_runtime_decision_returns_valid_action(self) -> None:
        from linkedin_cli import runtime_contract

        self.llm_providers.save_provider_config(
            provider_name="cerebras",
            api_key="secret-token",
            model="gpt-oss-120b",
        )
        request = runtime_contract.build_runtime_request(
            task_type="content_publish",
            objective="Choose the strongest candidate.",
            context={"industry": "ai", "topics": ["workflow"]},
            actions=[
                {
                    "action_id": "cand_001",
                    "action_type": "publish_post",
                    "payload": {"candidate_id": "cand_001", "text": "Post one"},
                    "score_snapshot": {"predicted_outcome_score": 5.2},
                },
                {
                    "action_id": "noop",
                    "action_type": "noop",
                    "payload": {},
                    "score_snapshot": {"predicted_outcome_score": 0.0},
                },
            ],
        )

        with patch("linkedin_cli.llm_providers.requests.post") as post_mock:
            post_mock.return_value.status_code = 200
            post_mock.return_value.json.return_value = {
                "choices": [
                    {
                        "message": {
                            "content": json.dumps(
                                {
                                    "request_id": request["request_id"],
                                    "request_fingerprint": request["request_fingerprint"],
                                    "chosen_action_id": "cand_001",
                                    "action_type": "publish_post",
                                    "execute": False,
                                    "rationale": "Candidate one has the strongest evidence and predicted outcome.",
                                    "payload": {"candidate_id": "cand_001", "text": "Post one"},
                                }
                            )
                        }
                    }
                ]
            }
            decision = self.llm_providers.decide_runtime_action_via_provider(
                provider_name="cerebras",
                request=request,
            )

        self.assertEqual(decision["response"]["chosen_action_id"], "cand_001")
        self.assertEqual(decision["response"]["action_type"], "publish_post")
        self.assertEqual(decision["provider"], "cerebras")
