# Autonomous Content Runtime Contract

The local autonomy stack is split into five layers:

- corpus
- generator
- rankers
- policy
- executors

The generator does not publish or message anyone directly. It receives a structured runtime request, chooses from a bounded action list, and returns JSON that the CLI can validate before execution.

## Request Envelope

Every runtime request contains:

- `request_id`
- `task_type`
- `objective`
- `context`
- `actions`
- `reward_spec`

Each action contains:

- `action_id`
- `action_type`
- `label`
- `payload`
- `score_snapshot`
- `metadata`

Allowed `action_type` values in the current contract:

- `generate_candidates`
- `choose_candidate`
- `publish_post`
- `queue_only`
- `reply_comment`
- `send_dm`
- `noop`

## Response Envelope

The model must return JSON only:

```json
{
  "request_id": "rt_123",
  "chosen_action_id": "cand_001",
  "action_type": "publish_post",
  "execute": true,
  "rationale": "Candidate 1 has the strongest score and best fit for the slice.",
  "payload": {
    "candidate_id": "cand_001"
  }
}
```

The CLI validates:

- matching `request_id`
- chosen action exists
- `action_type` matches the chosen action
- `execute` is boolean
- `payload` is an object

## Reward Spec

The current local content reward spec is `viral-v1`.

It combines:

- `reaction_log1p`
- `comment_log1p`
- `repost_log1p`
- `profile_view_log1p`
- `dm_reply`
- `meeting_booked`
- `negative_feedback`

This is intentionally broader than vanity engagement. The generator should optimize for distribution plus business signal, not only likes.

## Training Sources

SFT data is built from:

- chosen queued candidates
- owned posts
- harvested high-performing slice exemplars

Preference data is built from:

- chosen vs rejected candidates for the same prompt

This gives the generator three things:

- format learning from high-performing posts
- prompt-to-draft behavior from the queue
- explicit preference signal from ranked candidate sets
