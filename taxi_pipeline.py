"""dlt pipeline for NYC taxi data."""

import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig


# if no argument is provided, `access_token` is read from `.dlt/secrets.toml`
@dlt.source
def taxi_pipeline_rest_api_source(access_token: str = dlt.secrets.value):
    """Define dlt resources from REST API endpoints."""
    config: RESTAPIConfig = {
        "client": {
            # base URL for the taxi data API
            "base_url": "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api",
            # no authentication required for this public data
        },
        "resources": [
            {
                # single endpoint returning 1,000 records per page
                "name": "taxi_trips",
                "endpoint": {
                    # the API returns a plain JSON array of trips, no selector needed
                    # page-based pagination using `?page=1`, `?page=2`, ...
                    "paginator": {
                        "type": "page_number",
                        "page_param": "page",
                        "base_page": 1,
                        # the pager will stop automatically when an empty list is returned
                        "stop_after_empty_page": True,
                        "total_path": None,
                    },
                },
            }
        ],
        # no global defaults necessary for this simple source
    }

    yield from rest_api_resources(config)


pipeline = dlt.pipeline(
    pipeline_name="taxi_pipeline",
    destination="duckdb",
    dataset_name="nyc_taxi_data",
    # `refresh="drop_sources"` ensures the data and the state is cleaned
    # on each `pipeline.run()`; remove the argument once you have a
    # working pipeline.
    refresh="drop_sources",
    # show basic progress of resources extracted, normalized files and load-jobs on stdout
    progress="log",
)


if __name__ == "__main__":
    load_info = pipeline.run(taxi_pipeline_rest_api_source())
    print(load_info)  # noqa: T201
