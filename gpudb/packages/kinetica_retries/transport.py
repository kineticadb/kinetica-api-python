import logging
import sys
if sys.version_info >= (3, 9):
    from collections.abc import Callable, Coroutine
else:
    from typing import Callable, Coroutine

from functools import partial
from typing import Any, Optional, Union

import httpx

from .retry import Retry as Retry

logger = logging.getLogger(__name__)


class RetryTransport(httpx.BaseTransport):
    """
    A transport that automatically retries requests.

    ```python
    with httpx.Client(transport=RetryTransport()) as client:
        response = client.get("https://example.com")
    ```

    If you want to use a specific retry strategy, provide a [Retry][httpx_retries.Retry] configuration:

    ```python
    retry = Retry(total=5, backoff_factor=0.5)
    transport = RetryTransport(retry=retry)

    with httpx.Client(transport=transport) as client:
        response = client.get("https://example.com")
    ```

    By default, the implementation will create a sync transport internally, and use it for the request.
    If you want to configure your own transport, provide it to the `transport` argument:

    ```python
    transport = RetryTransport(transport=httpx.HTTPTransport(local_address="0.0.0.0"))
    ```

    Args:
        transport: Optional transport to wrap. If not provided, sync transport is created internally.
        retry: The retry configuration.
    """

    def __init__(
        self,
        transport: Optional[httpx.BaseTransport] = None,
        retry: Optional[Retry] = None,
    ) -> None:
        self.retry = retry or Retry()

        if transport is not None:
            self._sync_transport = transport if isinstance(transport, httpx.BaseTransport) else None
        else:
            self._sync_transport = httpx.HTTPTransport()

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        """
        Sends an HTTP request, possibly with retries.

        Args:
            request (httpx.Request): The request to send.

        Returns:
            The final response.
        """
        if self._sync_transport is None:
            raise RuntimeError("Synchronous request received but no sync transport available")

        logger.debug("handle_request started request=%s", request)

        if self.retry.is_retryable_method(request.method):
            send_method = partial(self._sync_transport.handle_request)
            response = self._retry_operation(request, send_method)
        else:
            response = self._sync_transport.handle_request(request)

        logger.debug("handle_request finished request=%s response=%s", request, response)

        return response

    def _retry_operation(
        self,
        request: httpx.Request,
        send_method: Callable[..., httpx.Response],
    ) -> httpx.Response:
        retry = self.retry
        response: Union[httpx.Response, httpx.HTTPError, None] = None

        while True:
            if response is not None:
                logger.debug("_retry_operation retrying response=%s retry=%s", response, retry)
                retry = retry.increment()
                retry.sleep(response)
            try:
                response = send_method(request)
            except httpx.HTTPError as e:
                if retry.is_exhausted() or not retry.is_retryable_exception(e):
                    raise

                response = e
                continue

            if retry.is_exhausted() or not retry.is_retryable_status_code(response.status_code):
                return response
