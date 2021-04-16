from typing import (
    Any,
    Dict,
    AsyncGenerator,
)

from ..base.base_call_builder import BaseCallBuilder
from ...client.base_async_client import BaseAsyncClient
from ...client.response import Response
from ...exceptions import raise_request_exception, NotPageableError
from ...utils import urljoin_with_query


class CallBuilder(BaseCallBuilder):
    """Creates a new :class:`BaseCallBuilder` pointed to server defined by horizon_url.

    This is an **abstract** class. Do not create this object directly, use :class:`stellar_sdk.server.Server` class.

    :param horizon_url: Horizon server URL.
    :param client: The client instance used to send request.
    """

    def __init__(self, horizon_url: str, client: BaseAsyncClient) -> None:
        super().__init__(horizon_url)
        if not isinstance(client, BaseAsyncClient):
            raise TypeError(
                "This `client` class should be an instance "
                "of `stellar_sdk.client.base_async_client.BaseAsyncClient`."
            )

        self.client: BaseAsyncClient = client

    async def call(self) -> Dict[str, Any]:
        """Triggers a HTTP request using this builder's current configuration.

        :return: If it is called synchronous, the response will be returned. If
            it is called asynchronously, it will return Coroutine.
        :raises:
            | :exc:`ConnectionError <stellar_sdk.exceptions.ConnectionError>`: if you have not successfully
                connected to the server.
            | :exc:`NotFoundError <stellar_sdk.exceptions.NotFoundError>`: if status_code == 404
            | :exc:`BadRequestError <stellar_sdk.exceptions.BadRequestError>`: if 400 <= status_code < 500
                and status_code != 404
            | :exc:`BadResponseError <stellar_sdk.exceptions.BadResponseError>`: if 500 <= status_code < 600
            | :exc:`UnknownRequestError <stellar_sdk.exceptions.UnknownRequestError>`: if an unknown error occurs,
                please submit an issue
        """
        url = urljoin_with_query(self.horizon_url, self.endpoint)
        return await self.__call(url, self.params)

    async def __call(self, url: str, params: dict = None) -> Dict[str, Any]:
        raw_resp = await self.client.get(url, params)  # type: ignore[misc]
        assert isinstance(raw_resp, Response)
        raise_request_exception(raw_resp)
        resp = raw_resp.json()
        self._check_pageable(resp)
        return resp

    async def stream(
            self,
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Creates an EventSource that listens for incoming messages from the server.

        See `Horizon Response Format <https://www.stellar.org/developers/horizon/reference/responses.html>`_

        See `MDN EventSource <https://developer.mozilla.org/en-US/docs/Web/API/EventSource>`_

        :return: If it is called synchronous, it will return ``Generator``, If
            it is called asynchronously, it will return ``AsyncGenerator``.

        :raise: :exc:`StreamClientError <stellar_sdk.exceptions.StreamClientError>` - Failed to fetch stream resource.
        """
        url = urljoin_with_query(self.horizon_url, self.endpoint)
        stream = self.client.stream(url, self.params)
        while True:
            yield await stream.__anext__()  # type: ignore[attr-defined]

    async def next(self) -> Dict[str, Any]:
        if self.next_href is None:
            raise NotPageableError("The next page does not exist.")
        return await self.__call(self.next_href, None)

    async def prev(self) -> Dict[str, Any]:
        if self.prev_href is None:
            raise NotPageableError("The prev page does not exist.")
        return await self.__call(self.prev_href, None)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, self.__class__):
            return NotImplemented  # pragma: no cover
        return (
                self.client == other.client
                and self.params == other.params
                and self.endpoint == other.endpoint
                and self.horizon_url == other.horizon_url
        )

    def __str__(self):
        return (
            f"<CallBuilder [horizon_url={self.horizon_url}, "
            f"endpoint={self.endpoint}, "
            f"params={self.params}, "
            f"prev_href={self.prev_href}, "
            f"next_href={self.next_href}, "
            f"client={self.client}]>"
        )
