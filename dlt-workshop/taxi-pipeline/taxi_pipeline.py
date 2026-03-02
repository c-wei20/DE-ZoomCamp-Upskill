import typing as t

import dlt
import requests


BASE_URL = "https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api"


@dlt.resource(name="nyc_taxi_data")
def nyc_taxi_data() -> t.Iterable[t.Sequence[dict]]:
    """Yield paginated NYC taxi data from the Zoomcamp demo API.

    The API returns JSON pages of up to 1,000 records.
    Pagination stops when an empty page is returned.
    """
    page = 1
    while True:
        response = requests.get(BASE_URL, params={"page": page}, timeout=30)
        response.raise_for_status()

        page_data = response.json()

        # Expect the endpoint to return a list of records for each page.
        if not page_data:
            break

        # Yield the whole page; dlt will normalize the list into rows.
        yield page_data

        page += 1


taxi_pipeline = dlt.pipeline(
    pipeline_name="taxi_pipeline",
    destination="duckdb",
    dataset_name="nyc_taxi",
    progress="log",
)


if __name__ == "__main__":
    load_info = taxi_pipeline.run(nyc_taxi_data())
    print(load_info)  # noqa: T201

