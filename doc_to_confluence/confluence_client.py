"""
Confluence Cloud REST API v2 client for the doc_to_confluence migration tool.

Authentication: HTTP Basic Auth using Atlassian account email + API token.
API reference: https://developer.atlassian.com/cloud/confluence/rest/v2/
Content format: Confluence storage format (XHTML subset).

Key design notes:
  - v2 API uses numeric spaceId, not spaceKey, for write operations.
    _get_space_id() resolves and caches this per-client-instance.
  - append_to_page() is a read-modify-write (get + concatenate + update)
    because the Confluence API has no native append endpoint.
  - _request() handles rate limiting (429) and server errors (5xx) with
    exponential backoff, up to MAX_RETRIES attempts.
"""
import time
from typing import List, Optional

import requests
from requests.auth import HTTPBasicAuth


class ConfluenceAPIError(Exception):
    """Raised when the Confluence API returns a non-2xx response."""

    def __init__(self, status_code: int, message: str, response_body: str = "") -> None:
        super().__init__(f"HTTP {status_code}: {message}")
        self.status_code = status_code
        self.response_body = response_body


class ConfluenceClient:
    """
    Confluence Cloud REST API v2 client.

    Usage:
        client = ConfluenceClient(
            base_url="https://myorg.atlassian.net",
            user="user@example.com",
            api_token="ATATT3x...",
        )
        page = client.get_page("123456")
        client.update_page("123456", page["title"], "<p>new content</p>", page["version"])
    """

    API_V2 = "/wiki/api/v2"
    MAX_RETRIES = 3
    BACKOFF_BASE = 2.0   # seconds; wait = BACKOFF_BASE ** attempt_number

    def __init__(
        self,
        base_url: str,
        user: str,
        api_token: str,
        timeout: int = 30,
    ) -> None:
        """
        Args:
            base_url: Confluence Cloud base URL, e.g. "https://myorg.atlassian.net"
            user: Atlassian account email address
            api_token: Atlassian API token
            timeout: HTTP request timeout in seconds
        """
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._space_id_cache: dict = {}
        self._user_cache: dict = {}        # account_id    → display_name
        self._user_name_cache: dict = {}   # display_name  → account_id (or None)

        self._session = requests.Session()
        self._session.auth = HTTPBasicAuth(user, api_token)
        self._session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json",
        })

    # ─── Public API ──────────────────────────────────────────────────────────

    def get_page(self, page_id: str, fmt: str = "storage") -> dict:
        """
        Fetch a page by ID including its body.

        GET /wiki/api/v2/pages/{page_id}?body-format={fmt}

        Args:
            page_id: Confluence page ID.
            fmt: Body format to return — ``"storage"`` (default, XHTML) or
                 ``"atlas_doc_format"`` (ADF JSON string).

        Returns:
            dict with keys: id, title, version (int), author_id, body, space_id, parent_id.
            When ``fmt="atlas_doc_format"`` the ``body`` value is a JSON *string*
            that can be passed to ``json.loads()``.
        """
        url = f"{self._base_url}{self.API_V2}/pages/{page_id}"
        resp = self._request("GET", url, params={"body-format": fmt})
        data = resp.json()
        version_obj = data.get("version", {})
        body_key = "atlas_doc_format" if fmt == "atlas_doc_format" else "storage"
        return {
            "id":        data.get("id"),
            "title":     data.get("title"),
            "version":   version_obj.get("number", 1),
            "author_id": version_obj.get("authorId", ""),   # Atlassian account ID
            "body":      data.get("body", {}).get(body_key, {}).get("value", ""),
            "space_id":  data.get("spaceId"),
            "parent_id": data.get("parentId"),
        }

    def create_page(
        self,
        space_key: str,
        title: str,
        content: str,
        parent_id: Optional[str] = None,
        representation: str = "storage",
    ) -> dict:
        """
        Create a new Confluence page.

        POST /wiki/api/v2/pages

        Args:
            space_key:      Space key (e.g. "PROJ") — resolved to numeric spaceId internally.
            title:          Page title.
            content:        Page body as a string.  For ``"storage"`` it is the XHTML
                            storage-format string (default, used by orchestrator and tracker).
                            For ``"atlas_doc_format"`` pass the JSON-serialised ADF document
                            (``json.dumps(adf_dict)``) and specify this explicitly.
            parent_id:      Optional parent page ID for nesting.
            representation: ``"storage"`` (default) or ``"atlas_doc_format"`` (pass
                            explicitly when writing ADF content, e.g. in apply_to_page).

        Returns:
            dict with keys: id, title, url
        """
        space_id = self._get_space_id(space_key)
        payload: dict = {
            "spaceId": space_id,
            "status":  "current",
            "title":   title,
            "body": {
                "representation": representation,
                "value":          content,
            },
        }
        if parent_id:
            payload["parentId"] = parent_id

        url = f"{self._base_url}{self.API_V2}/pages"
        resp = self._request("POST", url, json=payload)
        data = resp.json()
        page_id = data.get("id")
        return {
            "id":    page_id,
            "title": data.get("title"),
            "url":   f"{self._base_url}/wiki/spaces/{space_key}/pages/{page_id}",
        }

    def update_page(
        self,
        page_id: str,
        title: str,
        content: str,
        current_version: int,
        representation: str = "storage",
    ) -> dict:
        """
        Replace the full content of an existing page.

        PUT /wiki/api/v2/pages/{page_id}

        Args:
            page_id:         Confluence page ID.
            title:           Page title (required even if unchanged).
            content:         New page body as a string.  For ``"storage"`` it is the XHTML
                             storage-format string (default).  For ``"atlas_doc_format"``
                             pass the JSON-serialised ADF document and specify explicitly.
            current_version: Current version number (API requires version + 1).
            representation:  ``"storage"`` (default) or ``"atlas_doc_format"`` (pass
                             explicitly when writing ADF content).

        Returns:
            dict with keys: id, title, version
        """
        payload = {
            "id":     page_id,
            "status": "current",
            "title":  title,
            "body": {
                "representation": representation,
                "value":          content,
            },
            "version": {
                "number":  current_version + 1,
                "message": "Updated by doc_to_confluence migration tool",
            },
        }
        url = f"{self._base_url}{self.API_V2}/pages/{page_id}"
        resp = self._request("PUT", url, json=payload)
        data = resp.json()
        return {
            "id":      data.get("id"),
            "title":   data.get("title"),
            "version": data.get("version", {}).get("number"),
        }

    def append_to_page(self, page_id: str, new_content: str) -> dict:
        """
        Append new_content after existing page body content.

        Implemented as: GET current content → concatenate → PUT updated.

        Args:
            page_id: Target Confluence page ID
            new_content: Confluence storage format content to append

        Returns:
            dict with keys: id, title, version
        """
        current = self.get_page(page_id)
        combined = current["body"] + "\n" + new_content
        return self.update_page(
            page_id=page_id,
            title=current["title"],
            content=combined,
            current_version=current["version"],
        )

    def get_page_by_title(self, space_key: str, title: str) -> Optional[dict]:
        """
        Find a page by space key and exact title.

        GET /wiki/api/v2/pages?spaceId={id}&title={title}&body-format=storage

        Returns:
            Page dict (same shape as get_page()) or None if not found.
        """
        space_id = self._get_space_id(space_key)
        url = f"{self._base_url}{self.API_V2}/pages"
        params = {
            "spaceId": space_id,
            "title": title,
            "body-format": "storage",
        }
        resp = self._request("GET", url, params=params)
        results = resp.json().get("results", [])
        if not results:
            return None
        page = results[0]
        return {
            "id": page.get("id"),
            "title": page.get("title"),
            "version": page.get("version", {}).get("number", 1),
            "body": page.get("body", {}).get("storage", {}).get("value", ""),
        }

    def upload_attachment(
        self,
        page_id: str,
        filename: str,
        data_bytes: bytes,
        content_type: str = "application/octet-stream",
    ) -> dict:
        """
        Upload (or replace) a file attachment on a Confluence page.

        Uses the v1 REST API which supports multipart POST for attachments:
        POST /wiki/rest/api/content/{page_id}/child/attachment

        The v2 API (/wiki/api/v2/pages/{id}/attachments) returns 405 for POST
        on Confluence Cloud — attachments must go through v1.

        Args:
            page_id:      Target page ID
            filename:     Attachment filename (e.g. "image1.png")
            data_bytes:   Raw file bytes
            content_type: MIME type (e.g. "image/png")

        Returns:
            dict with keys: id, title, download_url
        """
        url = f"{self._base_url}/wiki/rest/api/content/{page_id}/child/attachment"

        # Multipart upload — temporarily strip the session's Content-Type header
        # so requests can set multipart/form-data with the correct boundary.
        # Also set X-Atlassian-Token: no-check to bypass XSRF protection.
        orig_ct = self._session.headers.pop("Content-Type", None)
        orig_token = self._session.headers.get("X-Atlassian-Token")
        self._session.headers["X-Atlassian-Token"] = "no-check"
        try:
            resp = self._request(
                "POST",
                url,
                files={"file": (filename, data_bytes, content_type)},
            )
        finally:
            if orig_ct is not None:
                self._session.headers["Content-Type"] = orig_ct
            if orig_token is None:
                self._session.headers.pop("X-Atlassian-Token", None)
            else:
                self._session.headers["X-Atlassian-Token"] = orig_token

        results = resp.json().get("results", [resp.json()])
        att = results[0] if results else resp.json()
        att_id = att.get("id", "")
        download = att.get("_links", {}).get("download", "")
        return {
            "id": att_id,
            "title": att.get("title", filename),
            "download_url": f"{self._base_url}/wiki{download}" if download else "",
        }

    def add_label_to_page(self, page_id: str, label: str) -> None:
        """
        Add a global label to a Confluence page (idempotent — safe to call
        even if the label already exists; Confluence returns 200 either way).

        Uses the v1 REST API:
            POST /wiki/rest/api/content/{page_id}/label
            Body: [{"prefix": "global", "name": "<label>"}]

        Label names must be lowercase alphanumeric + hyphens.

        Args:
            page_id: Confluence page ID.
            label:   Label name, e.g. ``"ds-tracked"``.

        Raises:
            ConfluenceAPIError: on 4xx/5xx responses (handled by _request).
        """
        url = f"{self._base_url}/wiki/rest/api/content/{page_id}/label"
        self._request("POST", url, json=[{"prefix": "global", "name": label}])

    def get_page_labels(self, page_id: str) -> List[str]:
        """
        Return the list of label names currently on a page.

        Uses the v1 REST API:
            GET /wiki/rest/api/content/{page_id}/label

        Args:
            page_id: Confluence page ID.

        Returns:
            List of label name strings (e.g. ``["ds-tracked", "use-case"]``).

        Raises:
            ConfluenceAPIError: on 4xx/5xx responses (handled by _request).
        """
        url = f"{self._base_url}/wiki/rest/api/content/{page_id}/label"
        resp = self._request("GET", url)
        return [item["name"] for item in resp.json().get("results", [])]

    def add_labels_to_page(self, page_id: str, labels: List[str]) -> None:
        """
        Add multiple global labels to a page in a single API call. Idempotent.

        Uses the v1 REST API:
            POST /wiki/rest/api/content/{page_id}/label
            Body: [{"prefix":"global","name":"label1"}, ...]

        Args:
            page_id: Confluence page ID.
            labels:  List of label names to add (lowercase, alphanumeric + hyphens).

        Raises:
            ConfluenceAPIError: on 4xx/5xx responses (handled by _request).
        """
        if not labels:
            return
        url = f"{self._base_url}/wiki/rest/api/content/{page_id}/label"
        self._request("POST", url, json=[{"prefix": "global", "name": lbl} for lbl in labels])

    def delete_page(self, page_id: str) -> None:
        """
        Permanently delete a Confluence page by ID.

        DELETE /wiki/api/v2/pages/{page_id}

        Deleted pages are moved to the Confluence trash (not immediately purged)
        and can be restored from the space trash by an admin.

        Args:
            page_id: Confluence page ID to delete.

        Raises:
            ConfluenceAPIError: on 4xx/5xx responses (handled by _request).
        """
        url = f"{self._base_url}{self.API_V2}/pages/{page_id}"
        self._request("DELETE", url)

    def get_child_pages(self, page_id: str) -> List[dict]:
        """
        Return the direct child pages of the given page (one level only).

        GET /wiki/api/v2/pages/{page_id}/children?limit=250&sort=title

        Handles cursor-based pagination automatically.

        Returns:
            List of dicts with keys: id, title, parent_id
        """
        results: List[dict] = []
        url = f"{self._base_url}{self.API_V2}/pages/{page_id}/children"
        params: dict = {"limit": 250, "sort": "title"}

        while url:
            resp = self._request("GET", url, params=params)
            data = resp.json()
            for page in data.get("results", []):
                results.append({
                    "id": page.get("id"),
                    "title": page.get("title"),
                    "parent_id": page.get("parentId"),
                })
            # Cursor-based pagination: next cursor lives in _links.next
            next_cursor = data.get("_links", {}).get("next")
            if next_cursor:
                # next_cursor is a relative path with cursor param already embedded
                url = f"{self._base_url}{next_cursor}"
                params = {}   # params are already in the URL
            else:
                url = ""

        return results

    def get_all_descendants(self, page_id: str) -> List[dict]:
        """
        Recursively collect all descendant pages under the given page.

        Performs a breadth-first traversal using get_child_pages().

        Returns:
            List of dicts with keys: id, title, parent_id (flattened, all depths)
        """
        all_pages: List[dict] = []
        queue: List[str] = [page_id]

        while queue:
            current_id = queue.pop(0)
            children = self.get_child_pages(current_id)
            for child in children:
                all_pages.append(child)
                queue.append(child["id"])   # recurse into children

        return all_pages

    def get_all_pages_in_space(self, space_key: str) -> List[dict]:
        """
        Fetch every current page in a space including its storage-format body.

        Uses the v2 API (``GET /wiki/api/v2/pages``) with cursor-based pagination
        — the same pattern as :meth:`get_child_pages` — so it is guaranteed to
        work on instances where the v1 ``/wiki/rest/api/content`` endpoint is
        unavailable or returns 404.

        Args:
            space_key: Space key, e.g. "DS".

        Returns:
            List of dicts with keys: id, title, body
        """
        space_id = self._get_space_id(space_key)
        url      = f"{self._base_url}{self.API_V2}/pages"
        results: List[dict] = []
        params: dict = {
            "spaceId":     space_id,
            "limit":       250,
            "body-format": "storage",
        }

        while url:
            resp = self._request("GET", url, params=params)
            data = resp.json()

            for item in data.get("results", []):
                ver = item.get("version", {})
                results.append({
                    "id":            str(item.get("id", "")),
                    "title":         item.get("title", ""),
                    "body":          item.get("body", {}).get("storage", {}).get("value", ""),
                    "version":       ver.get("number", 0),       # native Confluence revision
                    "last_modified": ver.get("createdAt", ""),   # ISO-8601 timestamp
                })

            # Cursor-based pagination — same as get_child_pages()
            next_cursor = data.get("_links", {}).get("next")
            url    = f"{self._base_url}{next_cursor}" if next_cursor else ""
            params = {}   # cursor URL already contains all params

        return results

    def find_user_by_name(self, display_name: str) -> Optional[str]:
        """
        Find an Atlassian account ID by searching for a user's display name.

        GET /wiki/rest/api/user/search?query={display_name}&maxResults=5

        Returns the account ID of the best match, or ``None`` when no match is
        found or the search API is unavailable.  Results are cached per client
        instance so repeated lookups for the same name (e.g. a shared approver
        across many pages) are free after the first call.

        Args:
            display_name: User's display name as shown in Confluence, e.g.
                          "Jane Smith".  Leading/trailing whitespace is stripped.

        Returns:
            Atlassian account ID string, or None on failure.
        """
        if not display_name:
            return None

        cache_key = display_name.strip().lower()
        if cache_key in self._user_name_cache:
            return self._user_name_cache[cache_key]

        url = f"{self._base_url}/wiki/rest/api/user/search"
        try:
            resp = self._request(
                "GET", url, params={"query": display_name.strip(), "maxResults": 5}
            )
            users = resp.json()
            # The v1 search returns a list of user objects
            if isinstance(users, list) and users:
                account_id: Optional[str] = users[0].get("accountId") or None
            else:
                account_id = None
        except Exception:
            account_id = None

        self._user_name_cache[cache_key] = account_id
        return account_id

    def get_user_display_name(self, account_id: str) -> str:
        """
        Resolve an Atlassian account ID to the user's display name.

        GET /wiki/rest/api/user?accountId={account_id}

        Results are cached per client instance so repeated lookups for the
        same user (common when many pages share one author) are free.

        Returns:
            Display name string (e.g. "Jane Smith"), or "" on any failure.
        """
        if not account_id:
            return ""

        if account_id in self._user_cache:
            return self._user_cache[account_id]

        url = f"{self._base_url}/wiki/rest/api/user"
        try:
            resp = self._request("GET", url, params={"accountId": account_id})
            display_name = resp.json().get("displayName", "")
        except Exception:
            display_name = ""

        self._user_cache[account_id] = display_name
        return display_name

    def resolve_or_create_folder_path(
        self,
        space_key: str,
        folder_path: str,
        root_parent_id: Optional[str] = None,
    ) -> str:
        """
        Walk a slash-separated folder hierarchy within a space, creating any
        missing folder pages along the way, and return the leaf page's ID.

        Each path segment is treated as a Confluence page title.  If a page
        with that title already exists under the current parent it is reused;
        otherwise a new blank page is created there.

        Args:
            space_key:       Space key, e.g. "PROJ"
            folder_path:     Slash-separated path, e.g. "Engineering/Backend"
            root_parent_id:  Optional starting parent page ID.  When None,
                             pages are created directly under the space root.

        Returns:
            The page ID of the deepest folder (last path segment).
        """
        segments = [s.strip() for s in folder_path.strip("/").split("/") if s.strip()]
        if not segments:
            raise ValueError(f"folder_path '{folder_path}' contains no valid segments")

        current_parent_id: Optional[str] = root_parent_id

        for segment in segments:
            # Try to find an existing page with this title under any parent.
            # Confluence enforces space-wide title uniqueness, so the page may
            # already exist even if it's under a different parent.
            existing = self.get_page_by_title(space_key, segment)
            if existing:
                current_parent_id = existing["id"]
                print(
                    f"[confluence_client] Folder '{segment}' already exists "
                    f"(id={current_parent_id})"
                )
            else:
                # Create a minimal folder page.
                # Confluence requires a non-empty storage-format body — an empty
                # string causes the page to appear broken in some Confluence versions.
                # A single non-breaking space paragraph is the conventional placeholder
                # used for container/folder pages.
                try:
                    created = self.create_page(
                        space_key=space_key,
                        title=segment,
                        content="<p>&nbsp;</p>",  # minimal valid body — container page
                        parent_id=current_parent_id,
                    )
                except ConfluenceAPIError as exc:
                    if exc.status_code == 400 and "title already exists" in exc.response_body.lower():
                        # Race condition or title returned by get_page_by_title was stale.
                        # Re-fetch and reuse the existing page.
                        print(
                            f"[confluence_client] Folder '{segment}' title conflict on create "
                            f"— re-fetching existing page"
                        )
                        existing = self.get_page_by_title(space_key, segment)
                        if existing:
                            current_parent_id = existing["id"]
                            continue
                    raise
                current_parent_id = created["id"]
                print(
                    f"[confluence_client] Created folder page '{segment}' "
                    f"(id={current_parent_id})"
                )

        return current_parent_id  # type: ignore[return-value]

    # ─── Private Helpers ─────────────────────────────────────────────────────

    def _get_space_id(self, space_key: str) -> str:
        """
        Resolve a space key to its numeric spaceId (required by v2 write API).

        GET /wiki/api/v2/spaces?keys={space_key}

        Results are cached per client instance to avoid redundant API calls.
        """
        if space_key in self._space_id_cache:
            return self._space_id_cache[space_key]

        url = f"{self._base_url}{self.API_V2}/spaces"
        resp = self._request("GET", url, params={"keys": space_key})
        results = resp.json().get("results", [])
        if not results:
            raise ConfluenceAPIError(
                404, f"Space with key '{space_key}' not found in Confluence"
            )
        space_id = str(results[0]["id"])
        self._space_id_cache[space_key] = space_id
        return space_id

    def _request(
        self,
        method: str,
        url: str,
        params: Optional[dict] = None,
        json: Optional[dict] = None,
        files: Optional[dict] = None,
    ) -> requests.Response:
        """
        Execute an HTTP request with retry and exponential backoff.

        Retries on:
          - 429 Too Many Requests (honours Retry-After header if present)
          - 5xx Server Errors

        Does NOT retry on 4xx Client Errors (except 429).

        Raises:
            ConfluenceAPIError on unretryable 4xx or after MAX_RETRIES failures.
        """
        last_exc: Optional[Exception] = None

        for attempt in range(self.MAX_RETRIES):
            try:
                response = self._session.request(
                    method=method,
                    url=url,
                    params=params,
                    json=json,
                    files=files,
                    timeout=self._timeout,
                )

                if response.status_code == 429:
                    wait = float(
                        response.headers.get("Retry-After", self.BACKOFF_BASE ** (attempt + 1))
                    )
                    print(
                        f"[confluence_client] Rate limited (429). "
                        f"Waiting {wait:.0f}s before retry {attempt + 1}/{self.MAX_RETRIES}..."
                    )
                    time.sleep(wait)
                    continue

                if response.status_code >= 500:
                    wait = self.BACKOFF_BASE ** (attempt + 1)
                    print(
                        f"[confluence_client] Server error {response.status_code}. "
                        f"Waiting {wait:.0f}s before retry {attempt + 1}/{self.MAX_RETRIES}..."
                    )
                    time.sleep(wait)
                    continue

                if response.status_code >= 400:
                    try:
                        err_body = response.json()
                        msg = (
                            err_body.get("message")
                            or err_body.get("title")
                            or response.text[:200]
                        )
                    except Exception:
                        msg = response.text[:200]
                    raise ConfluenceAPIError(response.status_code, msg, response.text)

                return response

            except ConfluenceAPIError:
                raise  # 4xx errors are not retried
            except requests.RequestException as exc:
                last_exc = exc
                wait = self.BACKOFF_BASE ** (attempt + 1)
                print(
                    f"[confluence_client] Request error: {exc}. "
                    f"Waiting {wait:.0f}s before retry {attempt + 1}/{self.MAX_RETRIES}..."
                )
                time.sleep(wait)

        raise ConfluenceAPIError(
            0,
            f"Request failed after {self.MAX_RETRIES} attempts: {last_exc}",
        )
