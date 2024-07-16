"""Stream type classes for tap-loyaltylion."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import parse_qsl, urlsplit

from dateutil import parser

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
    is_sorted = True
    check_sorted = False  # Skip checking sorting data
    start_date: str | None = None
    end_date: str | None = None
    STATE_MSG_FREQUENCY = (
        1000 * 1000 * 1000
    )  # Large value to disable write state message from within SDK

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

        params["updated_at_min"] = self.start_date
        params["updated_at_max"] = self.end_date
        params["limit"] = 500
        params["sort_field"] = "updated_at"
        self.logger.info(params)
        return params

    def get_records(self, context: dict) -> Iterable[dict[str, Any]]:
        """Return a generator of row-type dictionary objects.

        Args:
            context: The stream context.

        Yields:
            Each record from the source.
        """
        current_state = self.get_context_state(context)
        current_date = datetime.now(timezone.utc)
        date_window_size = float(self.config.get("max_fetch_interval", 1))
        min_value = current_state.get(
            "replication_key_value",
            self.config.get("start_date", ""),
        )
        context = context or {}
        min_date = parser.parse(min_value)
        while min_date < current_date:
            updated_at_max = min_date + timedelta(hours=date_window_size)
            if updated_at_max > current_date:
                updated_at_max = current_date

            self.start_date = min_date.isoformat()
            self.end_date = updated_at_max.isoformat()
            yield from super().get_records(context)
            # Send state message
            self._increment_stream_state({"updated_at": self.end_date}, context=context)
            self._write_state_message()
            min_date = updated_at_max


class TransactionsStream(LoyaltyLionStream):  # noqa: D101
    name = "transactions"
    path = "/transactions"
    primary_keys = ["id"]  # noqa: RUF012
    replication_key = "id"
    replication_method = "INCREMENTAL"
    records_jsonpath = "$.transactions[*]"
    schema_filepath = SCHEMAS_DIR / "transactions.json"
    check_sorted = False  # Skip checking sorting data
    is_sorted = True
    STATE_MSG_FREQUENCY = (
        1000 * 1000 * 1000
    )  # Large value to disable write state message from within SDK

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
        # Some transactions getting skipped because the way loyaltylion
        # adding their transactions in the system
        params["since_id"] = int(since_id) - 500
        params["limit"] = 500
        self.logger.info(params)
        return params
