"""Template for building a `dlt` pipeline to ingest data from a REST API."""

import dlt
from dlt.sources.rest_api import rest_api_resources
from dlt.sources.rest_api.typing import RESTAPIConfig


@dlt.source
def open_library_rest_api_source():
    """Define dlt resources from REST API endpoints."""
    config: RESTAPIConfig = {
        "client": {
            # Open Library base URL
            "base_url": "https://openlibrary.org/",
        },
        "resources": [
            {
                "name": "books",
                "endpoint": {
                    # single-book endpoint returning JSON metadata for one edition
                    "path": "books/OL7353617M.json",
                },
            },
        ],
        # set `resource_defaults` to apply configuration to all endpoints
    }

    yield from rest_api_resources(config)


pipeline = dlt.pipeline(
    pipeline_name='open_library_pipeline',
    destination='duckdb',
    # `refresh="drop_sources"` ensures the data and the state is cleaned
    # on each `pipeline.run()`; remove the argument once you have a
    # working pipeline.
    refresh="drop_sources",
    # show basic progress of resources extracted, normalized files and load-jobs on stdout
    progress="log",
)


if __name__ == "__main__":
    load_info = pipeline.run(open_library_rest_api_source())
    print(load_info)  # noqa: T201
