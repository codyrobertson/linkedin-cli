"""Deterministic LinkedIn-like sandbox for write-path tests.

This module intentionally does not try to mirror all of LinkedIn. It provides
the small authenticated HTTP surface that the CLI write planner, executor, and
reconcilers depend on, so tests can run realistic end-to-end flows without
touching a live LinkedIn account.
"""

from __future__ import annotations

import html
import json as json_lib
from itertools import count
from typing import Any
from urllib.parse import urlparse

from requests.cookies import cookiejar_from_dict


class SandboxResponse:
    """Small response object compatible with the parts of requests.Response we use."""

    def __init__(
        self,
        *,
        url: str,
        status_code: int = 200,
        payload: dict[str, Any] | list[Any] | None = None,
        text: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.url = url
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}
        if payload is not None and not self.headers:
            self.headers = {"content-type": "application/json"}
        if text is not None:
            self.text = text
        elif payload is not None:
            self.text = json_lib.dumps(payload)
        else:
            self.text = ""

    @property
    def ok(self) -> bool:
        return self.status_code < 400

    def json(self) -> dict[str, Any] | list[Any]:
        if self._payload is None:
            raise ValueError("Sandbox response does not contain JSON")
        return self._payload


class LinkedInSandboxSession:
    """requests.Session-shaped adapter backed by LinkedInSandbox state."""

    def __init__(self, sandbox: "LinkedInSandbox") -> None:
        self.sandbox = sandbox
        self.headers: dict[str, str] = {"User-Agent": "linkedin-cli-sandbox"}
        self.cookies = cookiejar_from_dict(
            {
                "li_at": "sandbox-li-at",
                "JSESSIONID": '"ajax:sandbox-csrf"',
            }
        )

    def request(self, method: str, url: str, **kwargs: Any) -> SandboxResponse:
        method = method.upper()
        if method == "GET":
            return self.get(url, **kwargs)
        if method == "POST":
            return self.post(url, **kwargs)
        if method == "PUT":
            return self.put(url, **kwargs)
        return self.sandbox.response(url=url, status_code=405, payload={"message": f"Unsupported method: {method}"})

    def get(self, url: str, **kwargs: Any) -> SandboxResponse:
        return self.sandbox.handle("GET", url, **kwargs)

    def post(self, url: str, **kwargs: Any) -> SandboxResponse:
        return self.sandbox.handle("POST", url, **kwargs)

    def put(self, url: str, **kwargs: Any) -> SandboxResponse:
        return self.sandbox.handle("PUT", url, **kwargs)


class LinkedInSandbox:
    """Stateful fake LinkedIn backend for local regression tests."""

    def __init__(
        self,
        *,
        account_id: str = "1708250765",
        member_hash: str = "sandbox-member",
        public_identifier: str = "sandbox-user",
    ) -> None:
        self.account_id = account_id
        self.member_hash = member_hash
        self.profile_urn = f"urn:li:fsd_profile:{member_hash}"
        self.mailbox_urn = self.profile_urn
        self.public_identifier = public_identifier
        self.default_conversation_urn = f"urn:li:msg_conversation:({self.mailbox_urn},2-sandbox)"
        self._ids = count(900000000001)
        self.requests: list[dict[str, Any]] = []
        self.profile: dict[str, Any] = {
            "headline": "Sandbox profile",
            "summary": "Local LinkedIn sandbox account",
            "locationName": "Phoenix, Arizona, United States",
            "websites": [],
        }
        self.positions: list[dict[str, Any]] = []
        self.posts: list[dict[str, Any]] = []
        self.comments_by_thread: dict[str, list[dict[str, Any]]] = {}
        self.messages: list[dict[str, Any]] = []
        self.uploads: dict[str, dict[str, Any]] = {}
        self.target_profiles: dict[str, dict[str, Any]] = {}
        self.add_target_profile("jane-sandbox", "urn:li:fsd_profile:jane-sandbox")
        self.session = LinkedInSandboxSession(self)

    def add_target_profile(self, vanity_name: str, target_urn: str | None = None) -> dict[str, Any]:
        profile = {
            "vanity_name": vanity_name,
            "target_urn": target_urn or f"urn:li:fsd_profile:{vanity_name}",
            "connection_state": "none",
            "follow_state": "none",
        }
        self.target_profiles[vanity_name] = profile
        return profile

    def seed_post(self, text: str, *, visibility: str = "ANYONE") -> dict[str, Any]:
        return self._create_post(text=text, visibility=visibility, media=[])

    def response(
        self,
        *,
        url: str,
        status_code: int = 200,
        payload: dict[str, Any] | list[Any] | None = None,
        text: str | None = None,
        headers: dict[str, str] | None = None,
    ) -> SandboxResponse:
        return SandboxResponse(
            url=url,
            status_code=status_code,
            payload=payload,
            text=text,
            headers=headers,
        )

    def handle(self, method: str, url: str, **kwargs: Any) -> SandboxResponse:
        parsed = urlparse(url)
        path = parsed.path
        body = kwargs.get("json")
        self.requests.append(
            {
                "method": method,
                "url": url,
                "path": path,
                "json": body,
                "data_length": len(kwargs.get("data") or b"") if method == "PUT" else None,
            }
        )

        if method == "GET":
            return self._handle_get(url, path)
        if method == "POST":
            return self._handle_post(url, path, body if isinstance(body, dict) else {})
        if method == "PUT":
            return self._handle_put(url, kwargs.get("data") or b"")
        return self.response(url=url, status_code=405, payload={"message": f"Unsupported method: {method}"})

    def _handle_get(self, url: str, path: str) -> SandboxResponse:
        if path == "/voyager/api/me":
            return self.response(url=url, payload=self._me_payload())
        if path == "/voyager/api/feed/normalizedUgcPosts":
            return self.response(url=url, payload={"included": self._feed_items()})
        if path == "/voyager/api/messaging/conversations":
            return self.response(url=url, payload={"included": list(self.messages)})
        if path.startswith("/in/"):
            vanity_name = path.strip("/").split("/", 1)[1]
            return self.response(
                url=url,
                text=self._profile_page_html(vanity_name),
                headers={"content-type": "text/html"},
            )
        if path.startswith("/posts/"):
            return self.response(
                url=url,
                text=self._post_page_html(url),
                headers={"content-type": "text/html"},
            )
        return self.response(url=url, status_code=404, payload={"message": f"No sandbox route for GET {path}"})

    def _handle_post(self, url: str, path: str, body: dict[str, Any]) -> SandboxResponse:
        query_id = str(body.get("queryId") or "")
        if path == "/voyager/api/graphql" and "voyagerContentcreationDashShares" in query_id:
            return self._handle_share_publish(url, body)
        if path == "/voyager/api/graphql" and "voyagerIdentityDashProfileEditFormPages" in query_id:
            return self._handle_profile_edit(url, body)
        if path == "/voyager/api/identity/dash/profilePositions":
            return self._handle_experience_add(url, body)
        if path == "/mwlite/profile/api/non-self/runQuery":
            return self._handle_mwlite_mutation(url, body)
        if path == "/voyager/api/voyagerMessagingDashMessengerMessages":
            return self._handle_dm_send(url, body)
        if path == "/voyager/api/voyagerMediaUploadMetadata":
            return self._handle_upload_registration(url, body)
        if path == "/voyager/api/voyagerSocialDashNormComments":
            return self._handle_comment_post(url, body)
        if path == "/voyager/api/graphql":
            return self.response(url=url, payload={"data": {"ok": True}})
        return self.response(url=url, status_code=404, payload={"message": f"No sandbox route for POST {path}"})

    def _handle_put(self, url: str, data: bytes) -> SandboxResponse:
        upload = self.uploads.get(url)
        if upload is None:
            return self.response(url=url, status_code=404, payload={"message": "Unknown upload URL"})
        upload["data"] = data
        upload["uploaded"] = True
        return self.response(url=url, status_code=201, payload={"ok": True})

    def _handle_share_publish(self, url: str, body: dict[str, Any]) -> SandboxResponse:
        post_input = ((body.get("variables") or {}).get("post") or {})
        commentary = post_input.get("commentary") or {}
        text = str(commentary.get("text") or "")
        visibility = str(((post_input.get("visibilityDataUnion") or {}).get("visibilityType")) or "ANYONE")
        media = [item.get("media") for item in post_input.get("media") or [] if isinstance(item, dict)]
        post = self._create_post(text=text, visibility=visibility, media=[item for item in media if item])
        payload = {
            "data": {
                "data": {
                    "createContentcreationDashShares": {
                        "resourceKey": post["entityUrn"],
                        "*entity": post["entityUrn"],
                    }
                }
            },
            "included": [
                {
                    "$type": "com.linkedin.voyager.dash.feed.Update",
                    "entityUrn": post["entityUrn"],
                    "activityUrn": post["activityUrn"],
                    "commentary": {"text": text},
                }
            ],
        }
        return self.response(url=url, payload=payload)

    def _handle_profile_edit(self, url: str, body: dict[str, Any]) -> SandboxResponse:
        inputs = ((body.get("variables") or {}).get("formElementInputs") or [])
        for item in inputs:
            if not isinstance(item, dict):
                continue
            form_urn = str(item.get("formElementUrn") or "")
            values = item.get("formElementInputValues") or []
            first = values[0] if values and isinstance(values[0], dict) else {}
            value = first.get("textInputValue") or first.get("locationInputValue")
            if value is None:
                continue
            if "/headline" in form_urn:
                self.profile["headline"] = value
            elif "/summary" in form_urn:
                self.profile["summary"] = value
            elif "/websiteUrl" in form_urn:
                self.profile["websites"] = [{"url": value}]
            elif "/geoLocation" in form_urn:
                self.profile["locationName"] = value
        return self.response(url=url, payload={"data": {"profileEdit": {"status": "OK"}}, "included": [self._profile_item()]})

    def _handle_experience_add(self, url: str, body: dict[str, Any]) -> SandboxResponse:
        entity_urn = f"urn:li:fsd_position:{next(self._ids)}"
        position = {
            "$type": "com.linkedin.voyager.dash.identity.profile.Position",
            "entityUrn": entity_urn,
            "title": body.get("title") or {"localized": {"en_US": ""}},
            "companyName": body.get("companyName") or {"localized": {"en_US": ""}},
        }
        for key in ("description", "locationName", "dateRange"):
            if key in body:
                position[key] = body[key]
        self.positions.append(position)
        return self.response(url=url, status_code=201, payload={"entityUrn": entity_urn, "data": {"entityUrn": entity_urn}})

    def _handle_mwlite_mutation(self, url: str, body: dict[str, Any]) -> SandboxResponse:
        variables = body.get("variables") or {}
        if "inviteeVanityName" in variables:
            vanity_name = str(variables.get("inviteeVanityName") or "")
            profile = self.target_profiles.get(vanity_name) or self.add_target_profile(vanity_name)
            profile["connection_state"] = "pending"
            return self.response(url=url, payload={"graphQL": {"addConnection": {"responseCode": "CREATED_201"}}})
        if variables.get("followState") == "FOLLOW_ACTIVE":
            target_urn = str(variables.get("to") or "")
            profile = self._target_profile_by_urn(target_urn)
            profile["follow_state"] = "following"
            return self.response(url=url, payload={"graphQL": {"updateFollowState": {"responseCode": "OK_200"}}})
        return self.response(url=url, status_code=400, payload={"message": "Unsupported mwlite mutation"})

    def _handle_dm_send(self, url: str, body: dict[str, Any]) -> SandboxResponse:
        message = body.get("message") or {}
        message_body = message.get("body") or {}
        text = str(message_body.get("text") or "")
        recipients = body.get("hostRecipientUrns") or []
        conversation_urn = body.get("conversationUrn")
        if not conversation_urn:
            recipient_key = recipients[0] if recipients else "unknown"
            conversation_urn = f"urn:li:msg_conversation:({self.mailbox_urn},{recipient_key})"
        message_urn = f"urn:li:msg:{next(self._ids)}"
        item = {
            "$type": "com.linkedin.voyager.messaging.Message",
            "entityUrn": message_urn,
            "*conversation": conversation_urn,
            "conversationUrn": conversation_urn,
            "body": {"text": text},
        }
        self.messages.insert(0, item)
        return self.response(url=url, status_code=201, payload={"data": {"value": {"entityUrn": message_urn}}})

    def _handle_upload_registration(self, url: str, body: dict[str, Any]) -> SandboxResponse:
        upload_id = next(self._ids)
        image_urn = f"urn:li:digitalmediaAsset:{upload_id}"
        upload_url = f"https://www.linkedin.com/sandbox-upload/{upload_id}"
        self.uploads[upload_url] = {
            "urn": image_urn,
            "body": body,
            "uploaded": False,
            "data": None,
        }
        return self.response(url=url, payload={"value": {"uploadUrl": upload_url, "urn": image_urn}})

    def _handle_comment_post(self, url: str, body: dict[str, Any]) -> SandboxResponse:
        commentary = body.get("commentary") or {}
        text = str(commentary.get("text") or "")
        thread_urn = str(body.get("threadUrn") or "")
        comment_urn = f"urn:li:comment:{next(self._ids)}"
        item = {
            "$type": "com.linkedin.voyager.dash.social.Comment",
            "urn": comment_urn,
            "entityUrn": comment_urn,
            "commentary": {"text": text},
            "commenter": {
                "title": {"text": "Sandbox User"},
                "navigationUrl": f"https://www.linkedin.com/in/{self.public_identifier}/",
            },
            "threadUrn": thread_urn,
        }
        self.comments_by_thread.setdefault(thread_urn, []).insert(0, item)
        return self.response(url=url, status_code=201, payload={"entityUrn": comment_urn, "data": {"entityUrn": comment_urn}})

    def _create_post(self, *, text: str, visibility: str, media: list[str]) -> dict[str, Any]:
        activity_id = str(next(self._ids))
        post = {
            "entityUrn": f"urn:li:ugcPost:{activity_id}",
            "activityUrn": f"urn:li:activity:{activity_id}",
            "url": f"https://www.linkedin.com/posts/sandbox-activity-{activity_id}",
            "text": text,
            "visibility": visibility,
            "media": list(media),
        }
        self.posts.insert(0, post)
        return post

    def _feed_items(self) -> list[dict[str, Any]]:
        return [
            {
                "$type": "com.linkedin.voyager.dash.feed.Update",
                "entityUrn": post["entityUrn"],
                "activityUrn": post["activityUrn"],
                "commentary": {"text": post["text"]},
                "media": post.get("media") or [],
            }
            for post in self.posts
        ]

    def _profile_item(self) -> dict[str, Any]:
        return {
            "$type": "com.linkedin.voyager.dash.identity.profile.Profile",
            "entityUrn": self.profile_urn,
            "objectUrn": f"urn:li:member:{self.account_id}",
            "publicIdentifier": self.public_identifier,
            "headline": self.profile.get("headline"),
            "summary": self.profile.get("summary"),
            "locationName": self.profile.get("locationName"),
            "geoLocationName": self.profile.get("locationName"),
            "websites": list(self.profile.get("websites") or []),
        }

    def _me_payload(self) -> dict[str, Any]:
        return {
            "data": {
                "plainId": self.account_id,
                "entityUrn": self.profile_urn,
                "publicIdentifier": self.public_identifier,
            },
            "included": [self._profile_item(), *self.positions],
        }

    def _target_profile_by_urn(self, target_urn: str) -> dict[str, Any]:
        for profile in self.target_profiles.values():
            if profile["target_urn"] == target_urn:
                return profile
        vanity_name = target_urn.rsplit(":", 1)[-1] or "sandbox-target"
        return self.add_target_profile(vanity_name, target_urn)

    def _profile_page_html(self, vanity_name: str) -> str:
        profile = self.target_profiles.get(vanity_name) or self.add_target_profile(vanity_name)
        connection_label = "Connect"
        if profile.get("connection_state") == "pending":
            connection_label = "Pending"
        elif profile.get("connection_state") == "connected":
            connection_label = "1st"
        follow_label = "Following" if profile.get("follow_state") == "following" else "Follow"
        return (
            "<html><body>"
            f"<main data-profile='{html.escape(vanity_name)}'>"
            f"<button>{html.escape(connection_label)}</button>"
            f"<button>{html.escape(follow_label)}</button>"
            "</main>"
            "</body></html>"
        )

    def _post_page_html(self, url: str) -> str:
        post = next((item for item in self.posts if item["url"] == url), None)
        comments: list[dict[str, Any]] = []
        support_items: list[dict[str, Any]] = []
        if post:
            comments.extend(self.comments_by_thread.get(post["entityUrn"], []))
            comments.extend(self.comments_by_thread.get(post["activityUrn"], []))
            support_items.extend(
                [
                    {
                        "$type": "com.linkedin.voyager.dash.social.SocialDetail",
                        "entityUrn": f"urn:li:fsd_socialDetail:({post['entityUrn']},urn:li:activity:{post['activityUrn'].rsplit(':', 1)[-1]})",
                    },
                    {
                        "$type": "com.linkedin.voyager.dash.social.SocialPermissions",
                        "canPostComments": True,
                    },
                ]
            )
        body = {"included": [*support_items, *comments]}
        body_id = "sandbox-comments-body"
        meta = {"request": "/voyager/api/graphql?sandboxComments", "status": 200, "body": body_id}
        return (
            "<html><body>"
            f"<code id='datalet-bpr-guid-1'>{html.escape(json_lib.dumps(meta))}</code>"
            f"<code id='{body_id}'>{html.escape(json_lib.dumps(body))}</code>"
            "</body></html>"
        )


__all__ = ["LinkedInSandbox", "LinkedInSandboxSession", "SandboxResponse"]
