"""Stream type classes for tap-loyaltylion."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlsplit

from tap_loyaltylion.client import LoyaltyLionStream

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class CustomersStream(LoyaltyLionStream):  # noqa: D101
    name = "customers"
    path = "/customers"
    primary_keys = ["id"]  # noqa: RUF012
    replication_key = "updated_at"
    replication_method = "INCREMENTAL"
    records_jsonpath = "$.customers[*]"
    schema_filepath = SCHEMAS_DIR / "customers.json"
    is_sorted = False

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: Any | None,  # noqa: ANN401
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict = {}
        if next_page_token:
            return dict(parse_qsl(urlsplit(next_page_token).query))

        # Get replication date from state
        context_state = self.get_context_state(context)
        last_updated = context_state.get("replication_key_value")

        config_start_date = self.config.get("start_date")
        start_date = last_updated if last_updated else config_start_date
        params["updated_at_min"] = start_date
        params["limit"] = 500
        self.logger.info(params)
        return params


class TransactionsStream(LoyaltyLionStream):  # noqa: D101
    name = "transactions"
    path = "/transactions"
    primary_keys = ["id"]  # noqa: RUF012
    replication_key = "id"
    replication_method = "INCREMENTAL"
    records_jsonpath = "$.transactions[*]"
    schema_filepath = SCHEMAS_DIR / "transactions.json"
    is_sorted = True

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: Any | None,  # noqa: ANN401
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict = {}
        if next_page_token:
            return dict(parse_qsl(urlsplit(next_page_token).query))

        # Get replication date from state
        context_state = self.get_context_state(context)
        last_updated = context_state.get("replication_key_value")

        since = self.config.get("since_id")
        since_id = last_updated if last_updated else since
        params["since_id"] = since_id
        params["limit"] = 500
        self.logger.info(params)
        return params

