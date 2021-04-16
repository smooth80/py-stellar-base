import abc
from typing import (
    Union,
    Dict,
    Mapping,
    Optional, TypeVar,
)

TBaseCallBuilder = TypeVar("TBaseCallBuilder", bound="BaseCallBuilder")


class BaseCallBuilder(abc.ABC):
    def __init__(self, horizon_url) -> None:
        self.horizon_url: str = horizon_url
        self.params: Dict[str, str] = {}
        self.endpoint: str = ""
        self.prev_href: Optional[str] = None
        self.next_href: Optional[str] = None

    def cursor(self: TBaseCallBuilder, cursor: Union) -> TBaseCallBuilder:
        """Sets ``cursor`` parameter for the current call. Returns the CallBuilder object on which this method has been called.

        See `Paging <https://www.stellar.org/developers/horizon/reference/paging.html>`_

        :param cursor: A cursor is a value that points to a specific location in a collection of resources.
        :return: current CallBuilder instance
        """
        self._add_query_param("cursor", cursor)
        return self

    def limit(self: TBaseCallBuilder, limit: int) -> TBaseCallBuilder:
        """Sets ``limit`` parameter for the current call. Returns the CallBuilder object on which this method has been called.

        See `Paging <https://www.stellar.org/developers/horizon/reference/paging.html>`_

        :param limit: Number of records the server should return.
        :return:
        """
        self._add_query_param("limit", limit)
        return self

    def order(self: TBaseCallBuilder, desc: bool = True) -> TBaseCallBuilder:
        """Sets ``order`` parameter for the current call. Returns the CallBuilder object on which this method has been called.

        :param desc: Sort direction, ``True`` to get desc sort direction, the default setting is ``True``.
        :return: current CallBuilder instance
        """
        order = "asc"
        if desc:
            order = "desc"
        self._add_query_param("order", order)
        return self

    def _add_query_param(self: TBaseCallBuilder, key: str, value: Union[str, float, int, bool, None]) -> None:
        if value is None:
            pass  # pragma: no cover
        elif value is True:
            self.params[key] = "true"
        elif value is False:
            self.params[key] = "false"
        else:
            self.params[key] = str(value)

    def _check_pageable(self: TBaseCallBuilder, response: dict) -> None:
        links = response.get("_links")
        if not links:
            return
        prev_page = links.get("prev")
        next_page = links.get("next")
        if prev_page:
            self.prev_href = prev_page.get("href")
        if next_page:
            self.next_href = next_page.get("href")

    def _add_query_params(
            self: TBaseCallBuilder, params: Mapping[str, Union[str, float, int, bool, None]]
    ) -> None:
        for k, v in params.items():
            self._add_query_param(k, v)
