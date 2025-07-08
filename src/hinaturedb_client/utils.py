import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tenacity import RetryCallState

logger = logging.getLogger(__name__)


def log_retry_attempt(retry_state: RetryCallState) -> None:
    if retry_state.attempt_number < 1:
        return
    logger.warning(
        f"Retry attempt: {retry_state.attempt_number}, \
        exception: {retry_state.outcome.exception() if retry_state.outcome else None}"
    )
