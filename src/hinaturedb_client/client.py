import datetime
from logging import get_logger
from typing import Dict, List
from urllib.parse import urljoin
from uuid import UUID

from httpx import AsyncClient, ConnectError, ConnectTimeout, HTTPStatusError, ReadTimeout, Timeout
from tenacity import RetryCallState, retry, retry_if_exception_type, stop_after_attempt, wait_exponential

logger = get_logger(__name__)


def log_retry_attempt(retry_state: RetryCallState) -> None:
    if retry_state.attempt_number < 1:
        return
    logger.warning(f"Retry attempt: {retry_state.attempt_number}, exception: {retry_state.outcome.exception()}")


class HinatureDBClient:
    def __init__(self, url, username: str, password: str):
        self.server_url = url
        self.server_username = username
        self.server_password = password
        self.hn_token = None

    async def refresh_token(self):
        if self.hn_token is None:
            await self.get_token()
        if self.expire < int(datetime.datetime.now(datetime.timezone.utc).timestamp()):
            await self.get_token()

    async def get_token(self):
        token_url = urljoin(self.server_url, "/api/v1/token")
        payload = {"grant_type": "password", "username": self.server_username, "password": self.server_password}
        async with AsyncClient() as client:
            res = await client.post(token_url, data=payload)
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
    async def create_records(self, records: List[Dict]) -> List[UUID]:
        url = urljoin(self.server_url, "/api/v1/record_batch")
        await self.refresh_token()
        headers = {
            "Authorization": f"Bearer {self.hn_token}",
        }
        async with AsyncClient(timeout=Timeout(30.0)) as client:
            res = await client.post(url, json=records, headers=headers)
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
    async def get_record(self, dataset_id: str, external_id: str, exact: bool = True) -> Dict | None:
        url = urljoin(self.server_url, "/api/v1/record")
        await self.refresh_token()
        params = {"dataset_id": dataset_id, "external_id": external_id, "exact": exact}
        headers = {
            "Authorization": f"Bearer {self.hn_token}",
            "Cache-Control": "no-store",
        }
        async with AsyncClient(timeout=Timeout(30.0)) as client:
            res = await client.get(url, params=params, headers=headers)
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
    async def get_records(self, dataset_id: UUID, external_ids: List[str]) -> List[Dict] | None:
        url = urljoin(self.server_url, "/api/v1/search_records")
        await self.refresh_token()
        payload = {
            "datasetID": dataset_id,
            "externalIDs": external_ids,
            "page": 0,
            "page_size": len(external_ids),
            "geom": False,
        }
        headers = {
            "Authorization": f"Bearer {self.hn_token}",
            "Cache-Control": "no-store",
        }
        async with AsyncClient(timeout=Timeout(30.0)) as client:
            res = await client.post(url, json=payload, headers=headers)
        res.raise_for_status()
        data = res.json()
        if not data["success"] and data["message"] == "Record not found":
            return None
        if data["success"]:
            return data["data"]
        else:
            raise Exception("Failed to fetch records")

    @retry(
        stop=stop_after_attempt(3),  # Retry up to 3 times
        wait=wait_exponential(multiplier=1, min=2, max=10),  # Wait 2s, 4s, 8s... up to 10s
        retry=retry_if_exception_type((ReadTimeout, ConnectTimeout, ConnectError)),  # Retry on these specific errors
        reraise=True,  # Reraise the exception if all retries fail
        before_sleep=log_retry_attempt,
    )
    async def batch_update_records(self, record_updates: List[Dict]) -> List[UUID]:
        url = urljoin(self.server_url, "/api/v1/records/batch")
        await self.refresh_token()
        headers = {
            "Authorization": f"Bearer {self.hn_token}",
        }
        record_updates = {"updates": record_updates}
        async with AsyncClient(timeout=Timeout(30)) as client:
            try:
                res = await client.put(url, json=record_updates, headers=headers, timeout=30)
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

    async def update_record(self, record_id: str, record: Dict) -> UUID:
        await self.refresh_token()
        headers = {
            "Authorization": f"Bearer {self.hn_token}",
        }
        url = urljoin(self.server_url, f"/api/v1/record/{record_id}")
        async with AsyncClient() as client:
            res = await client.put(url, json=record, headers=headers)
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
        params = {
            "page": 0,
            "page_size": 1,
            "name": name,
        }
        async with AsyncClient() as client:
            res = await client.get(url, params=params, headers=headers)
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
        params = {
            "page": 0,
            "page_size": 1,
            "name": name,
        }
        async with AsyncClient() as client:
            res = await client.get(url, params=params, headers=headers)
        res.raise_for_status()
        data = res.json()
        if not data["success"]:
            raise Exception("Failed to get dataset id: " + data["message"])
        else:
            return data["data"][0]
