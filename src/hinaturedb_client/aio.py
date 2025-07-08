import base64
import datetime
import logging
from typing import Any
from urllib.parse import urljoin
from uuid import UUID

from httpx import AsyncClient, ConnectError, ConnectTimeout, HTTPStatusError, ReadTimeout
from pybind11_geobuf import Decoder, Encoder
from shapely import to_geojson  # type: ignore
from shapely.geometry.base import BaseGeometry  # type: ignore
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from .utils import log_retry_attempt

logger = logging.getLogger(__name__)


class AsyncHinatureDBClient:
    def __init__(self, url: str, username: str, password: str, http_client: AsyncClient = AsyncClient()):
        self.server_url = url
        self.server_username = username
        self.server_password = password
        self.hn_token = None
        self.http_client = http_client
        self.geobuf_encoder = Encoder()
        self.geobuf_decoder = Decoder()

    async def refresh_token(self):
        if self.hn_token is None:
            await self.get_token()
        if self.expire < int(datetime.datetime.now(datetime.timezone.utc).timestamp()):
            await self.get_token()

    async def get_token(self):
        token_url = urljoin(self.server_url, "/api/v1/token")
        payload = {"grant_type": "password", "username": self.server_username, "password": self.server_password}
        res = await self.http_client.post(token_url, data=payload)
        res.raise_for_status()
        data = res.json()
        expires_in = data["expires_in"]
        now_time = int(datetime.datetime.now(datetime.timezone.utc).timestamp())
        self.expire = now_time + expires_in - 60
        self.hn_token = res.json()["access_token"]

        return self.hn_token

    @retry(
        stop=stop_after_attempt(3),  # Retry up to 3 times
        wait=wait_exponential(multiplier=1, min=2, max=10),  # Wait 2s, 4s, 8s... up to 10s
        retry=retry_if_exception_type((ReadTimeout, ConnectTimeout, ConnectError)),  # Retry on these specific errors
        reraise=True,  # Reraise the exception if all retries fail
        before_sleep=log_retry_attempt,
    )
    async def create_records(self, records: list[dict[str, Any]]) -> list[UUID]:
        url = urljoin(self.server_url, "/api/v1/record_batch")
        await self.refresh_token()
        headers = {
            "Authorization": f"Bearer {self.hn_token}",
        }
        res = await self.http_client.post(url, json=records, headers=headers, timeout=30)
        res.raise_for_status()
        data = res.json()
        if not data["success"]:
            raise Exception("Failed to create record " + data["message"])
        return data["data"]

    @retry(
        stop=stop_after_attempt(3),  # Retry up to 3 times
        wait=wait_exponential(multiplier=1, min=2, max=10),  # Wait 2s, 4s, 8s... up to 10s
        retry=retry_if_exception_type((ReadTimeout, ConnectTimeout, ConnectError)),  # Retry on these specific errors
        reraise=True,  # Reraise the exception if all retries fail
        before_sleep=log_retry_attempt,
    )
    async def get_record(self, dataset_id: str, external_id: str, exact: bool = True) -> dict[str, Any] | None:
        url = urljoin(self.server_url, "/api/v1/record")
        await self.refresh_token()
        params: dict[str, str | bool] = {"dataset_id": dataset_id, "external_id": external_id, "exact": exact}
        headers = {
            "Authorization": f"Bearer {self.hn_token}",
            "Cache-Control": "no-store",
        }
        res = await self.http_client.get(url, params=params, headers=headers, timeout=30)
        res.raise_for_status()
        data = res.json()
        if not data["success"] and data["message"] == "Record not found":
            return None
        if data["success"]:
            return data["data"]
        else:
            raise Exception("Failed to fetch record")

    @retry(
        stop=stop_after_attempt(3),  # Retry up to 3 times
        wait=wait_exponential(multiplier=1, min=2, max=10),  # Wait 2s, 4s, 8s... up to 10s
        retry=retry_if_exception_type((ReadTimeout, ConnectTimeout, ConnectError)),  # Retry on these specific errors
        reraise=True,  # Reraise the exception if all retries fail
        before_sleep=log_retry_attempt,
    )
    async def get_records(
        self,
        cursor: str | None = None,
        taxon_id: str | None = None,
        dataset_id: UUID | None = None,
        external_ids: list[str] | None = None,
        kingdom: str | None = None,
        location: str | BaseGeometry | None = None,
        distance: int | None = None,
        update_min: int | None = None,
        update_max: int | None = None,
        geom: bool = False,
        patch: bool = False,
    ) -> dict[str, Any]:
        url = urljoin(self.server_url, "/api/v1/records/search")
        await self.refresh_token()
        payload: dict[str, Any] = {
            "geom": geom,
            "patch": patch,
        }
        # 2. Fix the `len(external_ids)` bug and set a sensible page_size.
        if external_ids:
            payload["externalIDs"] = external_ids
            # If searching by IDs, page size should match the number of IDs.
            payload["page_size"] = len(external_ids)
        else:
            # Otherwise, use a default page size.
            payload["page_size"] = 50
        if taxon_id:
            payload["taxonID"] = taxon_id
        if dataset_id:
            # JSON does not have a UUID type, so convert it to a string.
            payload["datasetID"] = str(dataset_id)
        if kingdom:
            payload["kingdom"] = kingdom
        if location:
            if isinstance(location, BaseGeometry):
                location_geojson = to_geojson(location)
                location_b64_geobuf = base64.b64encode(self.geobuf_encoder.encode(location_geojson)).decode("utf-8")
                payload["location"] = location_b64_geobuf
            else:
                payload["location"] = location
        if distance:
            payload["distance"] = distance
        if update_min:
            payload["update_min"] = update_min
        if update_max:
            payload["update_max"] = update_max
        if cursor:
            payload["cursor"] = cursor
        headers = {
            "Authorization": f"Bearer {self.hn_token}",
            "Cache-Control": "no-store",
        }
        logger.debug("Fetching records from %s with payload: %s", url, payload)
        res = await self.http_client.post(url, json=payload, headers=headers, timeout=30)
        res.raise_for_status()
        data = res.json()
        return data

    async def get_all_records(
        self,
        taxon_id: str | None = None,
        dataset_id: UUID | None = None,
        external_ids: list[str] | None = None,
        kingdom: str | None = None,
        location: str | None = None,
        distance: int | None = None,
        update_min: int | None = None,
        update_max: int | None = None,
        geom: bool = False,
        patch: bool = False,
    ) -> list[dict[str, Any]] | None:
        records: list[dict[str, Any]] = []
        cursor = None
        while True:
            data = await self.get_records(
                cursor=cursor,
                taxon_id=taxon_id,
                dataset_id=dataset_id,
                external_ids=external_ids,
                kingdom=kingdom,
                location=location,
                distance=distance,
                update_min=update_min,
                update_max=update_max,
                geom=geom,
                patch=patch,
            )
            if not data["success"]:
                raise Exception("Failed to fetch record " + data["message"])
            else:
                records.extend(data["data"])
                if not data["has_more"]:
                    break
                else:
                    cursor = data.get("cursor")

        return records

    @retry(
        stop=stop_after_attempt(3),  # Retry up to 3 times
        wait=wait_exponential(multiplier=1, min=2, max=10),  # Wait 2s, 4s, 8s... up to 10s
        retry=retry_if_exception_type((ReadTimeout, ConnectTimeout, ConnectError)),  # Retry on these specific errors
        reraise=True,  # Reraise the exception if all retries fail
        before_sleep=log_retry_attempt,
    )
    async def batch_update_records(self, record_updates: list[dict[str, Any]]) -> list[UUID]:
        url = urljoin(self.server_url, "/api/v1/records/batch")
        await self.refresh_token()
        headers = {
            "Authorization": f"Bearer {self.hn_token}",
        }
        record_updates = {"updates": record_updates}  # type: ignore
        try:
            res = await self.http_client.put(url, json=record_updates, headers=headers, timeout=30)
            res.raise_for_status()
        except HTTPStatusError as e:
            error_details = "No response body."
            try:
                # Try to parse the response as JSON for detailed errors
                error_details = e.response.json()
            except Exception:
                # If not JSON, get the raw text
                error_details = e.response.text
            logger.error(
                f"HTTP error {e.response.status_code} for {e.request.url}: {error_details}",
                exc_info=False,  # Don't need the full traceback again here if logging details
            )
            # Re-raise the exception or handle it as needed
            raise Exception(f"Failed to update records. Server response: {error_details}") from e
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}", exc_info=True)
            raise  # Re-raise other unexpected errors
        data = res.json()
        if not data["success"]:
            raise Exception("Failed to update records: " + data["message"])
        return data["data"]

    async def update_record(self, record_id: str, record: dict[str, Any]) -> UUID:
        await self.refresh_token()
        headers = {
            "Authorization": f"Bearer {self.hn_token}",
        }
        url = urljoin(self.server_url, f"/api/v1/record/{record_id}")
        res = await self.http_client.put(url, json=record, headers=headers)
        res.raise_for_status()
        data = res.json()
        if not data["success"]:
            raise Exception("Failed to update record: " + data["message"])
        else:
            return data["data"]

    async def get_dataset_id(self, name: str) -> UUID:
        await self.refresh_token()
        headers = {
            "Authorization": f"Bearer {self.hn_token}",
        }
        url = urljoin(self.server_url, "/api/v1/datasets")
        params: dict[str, int | str] = {
            "page": 0,
            "page_size": 1,
            "name": name,
        }
        res = await self.http_client.get(url, params=params, headers=headers)
        res.raise_for_status()
        data = res.json()
        if not data["success"]:
            raise Exception("Failed to get dataset id: " + data["message"])
        else:
            return data["data"][0]["id"]

    async def get_dataset_by_name(self, name: str) -> UUID:
        await self.refresh_token()
        headers = {
            "Authorization": f"Bearer {self.hn_token}",
        }
        url = urljoin(self.server_url, "/api/v1/datasets")
        params: dict[str, int | str] = {
            "page": 0,
            "page_size": 1,
            "name": name,
        }
        res = await self.http_client.get(url, params=params, headers=headers)
        res.raise_for_status()
        data = res.json()
        if not data["success"]:
            raise Exception("Failed to get dataset id: " + data["message"])
        else:
            return data["data"][0]
