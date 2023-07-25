"""LoyaltyLion tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_loyaltylion import streams


class TapLoyaltyLion(Tap):
    """LoyaltyLion tap class."""

    name = "tap-loyaltylion"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "ll_username",
            th.StringType,
            required=True,
            secret=True,  # Flag config as protected.
            description="LoyaltyLion API Username",
        ),
        th.Property(
            "ll_password",
            th.StringType,
            required=True,
            secret=True,  # Flag config as protected.
            description="LoyaltyLion API Password",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync",
        ),
        th.Property(
            "since_id",
            th.IntegerType,
            description="Initial ID of the record to sync. Specifically for transactions",
        ),
        th.Property(
            "max_fetch_interval",
            th.IntegerType,
            description="Max number of hours of data to fetch in one run. Applies to customers stream only",
        ),
        th.Property(
            "api_url",
            th.StringType,
            default="https://api.loyaltylion.com/v2",
            description="The url for the API service",
        ),
    ).to_dict()

    def discover_streams(self) -> list[streams.LoyaltyLionStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.CustomersStream(self),
            streams.TransactionsStream(self),
        ]


if __name__ == "__main__":
    TapLoyaltyLion.cli()
