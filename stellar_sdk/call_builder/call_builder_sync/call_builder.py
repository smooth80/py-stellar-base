from typing import (
    Union,
    Any,
    Dict,
    Mapping,
    Generator,
    Optional,
)

from ...client.base_sync_client import BaseSyncClient
from ...client.response import Response
from ...exceptions import raise_request_exception, NotPageableError, TypeError
from ...utils import urljoin_with_query


class CallBuilder:
    """Creates a new :class:`BaseCallBuilder` pointed to server defined by horizon_url.

    This is an **abstract** class. Do not create this object directly, use :class:`stellar_sdk.server.Server` class.

    :param horizon_url: Horizon server URL.
    :param client: The client instance used to send request.
    """

    def __init__(self, horizon_url: str, client: BaseSyncClient) -> None:
        if not isinstance(client, BaseSyncClient):
            raise TypeError(
                "This `client` class should be an instance "
                "of `stellar_sdk.client.base_async_client.BaseSyncClient`."
            )
        self.client: BaseSyncClient = client
        self.horizon_url: str = horizon_url
        self.params: Dict[str, str] = {}
        self.endpoint: str = ""
        self.prev_href: Optional[str] = None
        self.next_href: Optional[str] = None

    def call(self) -> Dict[str, Any]:
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
        return self.__call(url, self.params)

    def __call(self, url: str, params: dict = None) -> Dict[str, Any]:
        raw_resp = self.client.get(url, params)
        assert isinstance(raw_resp, Response)
        raise_request_exception(raw_resp)
        resp = raw_resp.json()
        self._check_pageable(resp)
        return resp

    def stream(
            self,
    ) -> Generator[Dict[str, Any], None, None]:
        """Creates an EventSource that listens for incoming messages from the server.

        See `Horizon Response Format <https://www.stellar.org/developers/horizon/reference/responses.html>`_

        See `MDN EventSource <https://developer.mozilla.org/en-US/docs/Web/API/EventSource>`_

        :return: If it is called synchronous, it will return ``Generator``, If
            it is called asynchronously, it will return ``AsyncGenerator``.

        :raise: :exc:`StreamClientError <stellar_sdk.exceptions.StreamClientError>` - Failed to fetch stream resource.
        """
        url = urljoin_with_query(self.horizon_url, self.endpoint)
        return self.client.stream(url, self.params)  # type: ignore[return-value]


    def next(self):
        if self.next_href is None:
            raise NotPageableError("The next page does not exist.")
        return self.__call(self.next_href, None)

    def prev(self):
        if self.prev_href is None:
            raise NotPageableError("The prev page does not exist.")
        return self.__call(self.prev_href, None)

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
