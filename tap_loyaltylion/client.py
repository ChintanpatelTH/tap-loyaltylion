"""REST client handling, including LoyaltyLionStream base class."""

from __future__ import annotations

from typing import Any, Callable, Iterable, Optional

import requests
from singer_sdk.authenticators import BasicAuthenticator
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream

_Auth = Callable[[requests.PreparedRequest], requests.PreparedRequest]


class LoyaltyLionStream(RESTStream):
    """LoyaltyLion stream class."""

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self.config.get("api_url")

    records_jsonpath = "$[*]"

    next_page_token_jsonpath = "$.cursor.next"  # noqa: S105

    @property
    def authenticator(self) -> BasicAuthenticator:
        """Return a new authenticator object.

        Returns:
            An authenticator instance.
        """
        return BasicAuthenticator.create_for_stream(
            self,
            username=self.config.get("username", self.config.get("ll_username")),
            password=self.config.get("password", self.config.get("ll_password")),
        )

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Returns:
            A dictionary of HTTP headers.
        """
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        return headers

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]  # noqa: ARG002
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        next_link = response.links.get("next")
        if not next_link or not response.json():
            self.last_id = None
            return None

        self.logger.info(next_link["url"])
        return next_link["url"]

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """
        yield from extract_jsonpath(self.records_jsonpath, input=response.json())
