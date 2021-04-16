from typing import Union, Any, Dict, List, Tuple, Generator

from .account import Account, Thresholds
from .asset import Asset
from .base_transaction_envelope import BaseTransactionEnvelope
from stellar_sdk.call_builder.call_builder_async import AccountsCallBuilder
from stellar_sdk.call_builder.call_builder_async.assets_call_builder import AssetsCallBuilder
from stellar_sdk.call_builder.call_builder_async import (
    ClaimableBalancesCallBuilder,
)
from stellar_sdk.call_builder.call_builder_async.data_call_builder import DataCallBuilder
from stellar_sdk.call_builder.call_builder_async import EffectsCallBuilder
from stellar_sdk.call_builder.call_builder_async.fee_stats_call_builder import FeeStatsCallBuilder
from stellar_sdk.call_builder.call_builder_async.ledgers_call_builder import LedgersCallBuilder
from stellar_sdk.call_builder.call_builder_async import OffersCallBuilder
from stellar_sdk.call_builder.call_builder_async.operations_call_builder import OperationsCallBuilder
from stellar_sdk.call_builder.call_builder_async.orderbook_call_builder import OrderbookCallBuilder
from stellar_sdk.call_builder.call_builder_async.payments_call_builder import PaymentsCallBuilder
from stellar_sdk.call_builder.call_builder_async.root_call_builder import RootCallBuilder
from stellar_sdk.call_builder.call_builder_async.strict_receive_paths_call_builder import (
    StrictReceivePathsCallBuilder,
)
from stellar_sdk.call_builder.call_builder_async import (
    StrictSendPathsCallBuilder,
)
from stellar_sdk.call_builder.call_builder_async.trades_aggregation_call_builder import (
    TradeAggregationsCallBuilder,
)
from stellar_sdk.call_builder.call_builder_async.trades_call_builder import TradesCallBuilder
from stellar_sdk.call_builder.call_builder_async import TransactionsCallBuilder
from .client.base_async_client import BaseAsyncClient
from .client.aiohttp_client import AiohttpClient
from .client.response import Response
from .exceptions import TypeError, NotFoundError, raise_request_exception
from .fee_bump_transaction import FeeBumpTransaction
from .fee_bump_transaction_envelope import FeeBumpTransactionEnvelope
from .helpers import parse_transaction_envelope_from_xdr
from .keypair import Keypair
from .memo import NoneMemo
from .sep.exceptions import AccountRequiresMemoError
from .transaction import Transaction
from .transaction_envelope import TransactionEnvelope
from .utils import (
    urljoin_with_query,
    MUXED_ACCOUNT_STARTING_LETTER,
)
from .operation import (
    Payment,
    AccountMerge,
    PathPaymentStrictSend,
    PathPaymentStrictReceive,
)

__all__ = ["Server"]


class Server:
    """Server handles the network connection to a `Horizon <https://www.stellar.org/developers/horizon/reference/>`_
    instance and exposes an interface for requests to that instance.

    Here we need to talk about the **client** parameter, if you do not specify the client, we will use
    the :class:`stellar_sdk.client.requests_client.RequestsClient` instance by default, it is a synchronous HTTPClient,
    you can also specify an asynchronous HTTP Client,
    for example: :class:`stellar_sdk.client.aiohttp_client.AiohttpClient`. If you use a synchronous client,
    then all requests are synchronous. If you use an asynchronous client,
    then all requests are asynchronous. The choice is in your hands.

    :param horizon_url: Horizon Server URL (ex. `https://horizon-testnet.stellar.org`)
    :param client: Http Client used to send the request
    :raises: :exc:`TypeError <stellar_sdk.exceptions.TypeError>`: if the ``client`` does not meet the standard.
    """

    def __init__(
        self,
        horizon_url: str = "https://horizon-testnet.stellar.org/",
        client: BaseAsyncClient = None,
    ) -> None:
        self.horizon_url: str = horizon_url

        if not client:
            client = AiohttpClient()
        self._client: BaseAsyncClient = client

        if not isinstance(self._client, BaseAsyncClient):
            raise TypeError(
                "This `client` class should be an instance "
                "of `stellar_sdk.client.base_async_client.BaseAsyncClient`."
            )

    async def submit_transaction(
        self,
        transaction_envelope: Union[
            TransactionEnvelope, FeeBumpTransactionEnvelope, str
        ],
        skip_memo_required_check: bool = False,
    ) -> Dict[str, Any]:
        """Submits a transaction to the network.

        :param transaction_envelope: :class:`stellar_sdk.transaction_envelope.TransactionEnvelope` object
            or base64 encoded xdr
        :return: the response from horizon
        :raises:
            :exc:`ConnectionError <stellar_sdk.exceptions.ConnectionError>`
            :exc:`NotFoundError <stellar_sdk.exceptions.NotFoundError>`
            :exc:`BadRequestError <stellar_sdk.exceptions.BadRequestError>`
            :exc:`BadResponseError <stellar_sdk.exceptions.BadResponseError>`
            :exc:`UnknownRequestError <stellar_sdk.exceptions.UnknownRequestError>`
            :exc:`AccountRequiresMemoError <stellar_sdk.sep.exceptions.AccountRequiresMemoError>`
        """
        return await self.__submit_transaction(
            transaction_envelope, skip_memo_required_check
        )

    async def __submit_transaction(
        self,
        transaction_envelope: Union[
            TransactionEnvelope, FeeBumpTransactionEnvelope, str
        ],
        skip_memo_required_check: bool,
    ) -> Dict[str, Any]:
        url = urljoin_with_query(self.horizon_url, "transactions")
        xdr, tx = self.__get_xdr_and_transaction_from_transaction_envelope(
            transaction_envelope
        )
        if not skip_memo_required_check:
            await self.__check_memo_required(tx)
        data = {"tx": xdr}
        resp = await self._client.post(url=url, data=data)
        assert isinstance(resp, Response)
        raise_request_exception(resp)
        return resp.json()

    def __get_xdr_and_transaction_from_transaction_envelope(
        self,
        transaction_envelope: Union[
            TransactionEnvelope, FeeBumpTransactionEnvelope, str
        ],
    ) -> Tuple[str, Union[Transaction, FeeBumpTransaction]]:
        if isinstance(transaction_envelope, BaseTransactionEnvelope):
            xdr = transaction_envelope.to_xdr()
            tx = transaction_envelope.transaction
        else:
            xdr = transaction_envelope
            tx = parse_transaction_envelope_from_xdr(
                transaction_envelope, ""
            ).transaction
        return xdr, tx

    def root(self) -> RootCallBuilder:
        """
        :return: New :class:`stellar_sdk.call_builder_async.RootCallBuilder` object configured
            by a current Horizon server configuration.
        """
        return RootCallBuilder(horizon_url=self.horizon_url, client=self._client)

    def accounts(self) -> AccountsCallBuilder:
        """
        :return: New :class:`stellar_sdk.call_builder_async.AccountsCallBuilder` object configured
            by a current Horizon server configuration.
        """
        return AccountsCallBuilder(horizon_url=self.horizon_url, client=self._client)

    def assets(self) -> AssetsCallBuilder:
        """
        :return: New :class:`stellar_sdk.call_builder_async.AssetsCallBuilder` object configured by
            a current Horizon server configuration.
        """
        return AssetsCallBuilder(horizon_url=self.horizon_url, client=self._client)

    def claimable_balances(self) -> ClaimableBalancesCallBuilder:
        """
        :return: New :class:`stellar_sdk.call_builder_async.ClaimableBalancesCallBuilder` object configured by
            a current Horizon server configuration.
        """
        return ClaimableBalancesCallBuilder(
            horizon_url=self.horizon_url, client=self._client
        )

    def data(self, account_id: str, data_name: str):
        """
        :return: New :class:`stellar_sdk.call_builder_async.DataCallBuilder` object configured by
            a current Horizon server configuration.
        """
        return DataCallBuilder(
            horizon_url=self.horizon_url,
            client=self._client,
            account_id=account_id,
            data_name=data_name,
        )

    def effects(self) -> EffectsCallBuilder:
        """
        :return: New :class:`stellar_sdk.call_builder_async.EffectsCallBuilder` object configured by
            a current Horizon server configuration.
        """
        return EffectsCallBuilder(horizon_url=self.horizon_url, client=self._client)

    def fee_stats(self) -> FeeStatsCallBuilder:
        """
        :return: New :class:`stellar_sdk.call_builder_async.FeeStatsCallBuilder` object configured by
            a current Horizon server configuration.
        """
        return FeeStatsCallBuilder(horizon_url=self.horizon_url, client=self._client)

    def ledgers(self) -> LedgersCallBuilder:
        """
        :return: New :class:`stellar_sdk.call_builder_async.LedgersCallBuilder` object configured by
            a current Horizon server configuration.
        """
        return LedgersCallBuilder(horizon_url=self.horizon_url, client=self._client)

    def offers(self) -> OffersCallBuilder:
        """
        :return: New :class:`stellar_sdk.call_builder_async.OffersCallBuilder` object configured by
            a current Horizon server configuration.
        """
        return OffersCallBuilder(horizon_url=self.horizon_url, client=self._client)

    def operations(self) -> OperationsCallBuilder:
        """
        :return: New :class:`stellar_sdk.call_builder_async.OperationsCallBuilder` object configured by
            a current Horizon server configuration.
        """
        return OperationsCallBuilder(horizon_url=self.horizon_url, client=self._client)

    def orderbook(self, selling: Asset, buying: Asset) -> OrderbookCallBuilder:
        """
        :param selling: Asset being sold
        :param buying: Asset being bought
        :return: New :class:`stellar_sdk.call_builder_async.OrderbookCallBuilder` object configured by
            a current Horizon server configuration.
        """
        return OrderbookCallBuilder(
            horizon_url=self.horizon_url,
            client=self._client,
            buying=buying,
            selling=selling,
        )

    def strict_receive_paths(
        self,
        source: Union[str, List[Asset]],
        destination_asset: Asset,
        destination_amount: str,
    ):
        """
        :param source: The sender's account ID or a list of Assets. Any returned path must use a source that the sender can hold.
        :param destination_asset: The destination asset.
        :param destination_amount: The amount, denominated in the destination asset, that any returned path should be able to satisfy.
        :return: New :class:`stellar_sdk.call_builder_async.StrictReceivePathsCallBuilder` object configured by
            a current Horizon server configuration.
        """
        return StrictReceivePathsCallBuilder(
            horizon_url=self.horizon_url,
            client=self._client,
            source=source,
            destination_asset=destination_asset,
            destination_amount=destination_amount,
        )

    def strict_send_paths(
        self,
        source_asset: Asset,
        source_amount: str,
        destination: Union[str, List[Asset]],
    ):
        """
        :param source_asset: The asset to be sent.
        :param source_amount: The amount, denominated in the source asset, that any returned path should be able to satisfy.
        :param destination: The destination account or the destination assets.
        :return: New :class:`stellar_sdk.call_builder_async.StrictReceivePathsCallBuilder` object configured by
            a current Horizon server configuration.
        """
        return StrictSendPathsCallBuilder(
            horizon_url=self.horizon_url,
            client=self._client,
            source_asset=source_asset,
            source_amount=source_amount,
            destination=destination,
        )

    def payments(self) -> PaymentsCallBuilder:
        """
        :return: New :class:`stellar_sdk.call_builder_async.PaymentsCallBuilder` object configured by
            a current Horizon server configuration.
        """
        return PaymentsCallBuilder(horizon_url=self.horizon_url, client=self._client)

    def trade_aggregations(
        self,
        base: Asset,
        counter: Asset,
        resolution: int,
        start_time: int = None,
        end_time: int = None,
        offset: int = None,
    ) -> TradeAggregationsCallBuilder:
        """
        :param base: base asset
        :param counter: counter asset
        :param resolution: segment duration as millis since epoch. *Supported values
            are 1 minute (60000), 5 minutes (300000), 15 minutes (900000),
            1 hour (3600000), 1 day (86400000) and 1 week (604800000).*
        :param start_time: lower time boundary represented as millis since epoch
        :param end_time: upper time boundary represented as millis since epoch
        :param offset: segments can be offset using this parameter.
            Expressed in milliseconds. *Can only be used if the resolution is greater than 1 hour.
            Value must be in whole hours, less than the provided resolution, and less than 24 hours.*
        :return: New :class:`stellar_sdk.call_builder_async.TradeAggregationsCallBuilder` object configured by
            a current Horizon server configuration.
        """
        return TradeAggregationsCallBuilder(
            horizon_url=self.horizon_url,
            client=self._client,
            base=base,
            counter=counter,
            start_time=start_time,
            end_time=end_time,
            resolution=resolution,
            offset=offset,
        )

    def trades(self) -> TradesCallBuilder:
        """
        :return: New :class:`stellar_sdk.call_builder_async.TradesCallBuilder` object configured by
            a current Horizon server configuration.
        """
        return TradesCallBuilder(horizon_url=self.horizon_url, client=self._client)

    def transactions(self) -> TransactionsCallBuilder:
        """
        :return: New :class:`stellar_sdk.call_builder_async.TransactionsCallBuilder` object configured by
            a current Horizon server configuration.
        """
        return TransactionsCallBuilder(
            horizon_url=self.horizon_url, client=self._client
        )

    async def load_account(self, account_id: Union[Keypair, str]) -> Account:
        """Fetches an account's most current state in the ledger and then creates
        and returns an :class:`stellar_sdk.account.Account` object.

        :param account_id: The account to load.
        :return: an :class:`stellar_sdk.account.Account` object.
        :raises:
            :exc:`ConnectionError <stellar_sdk.exceptions.ConnectionError>`
            :exc:`NotFoundError <stellar_sdk.exceptions.NotFoundError>`
            :exc:`BadRequestError <stellar_sdk.exceptions.BadRequestError>`
            :exc:`BadResponseError <stellar_sdk.exceptions.BadResponseError>`
            :exc:`UnknownRequestError <stellar_sdk.exceptions.UnknownRequestError>`
        """

        if isinstance(account_id, Keypair):
            account = account_id.public_key
        else:
            account = account_id
        return await self.__load_account(account)

    async def __load_account(self, account_id: str) -> Account:
        resp = await self.accounts().account_id(account_id=account_id).call()
        assert isinstance(resp, dict)
        sequence = int(resp["sequence"])
        thresholds = Thresholds(
            resp["thresholds"]["low_threshold"],
            resp["thresholds"]["med_threshold"],
            resp["thresholds"]["high_threshold"],
        )
        account = Account(account_id=account_id, sequence=sequence)
        account.signers = resp["signers"]
        account.thresholds = thresholds
        return account

    async def __check_memo_required(
        self, transaction: Union[Transaction, FeeBumpTransaction]
    ) -> None:
        if isinstance(transaction, FeeBumpTransaction):
            transaction = transaction.inner_transaction_envelope.transaction
        if not (transaction.memo is None or isinstance(transaction.memo, NoneMemo)):
            return
        for index, destination in self.__get_check_memo_required_destinations(
            transaction
        ):
            if destination.startswith(MUXED_ACCOUNT_STARTING_LETTER):
                continue
            try:
                account_resp = await self.accounts().account_id(destination).call()  # type: ignore[misc]
            except NotFoundError:
                continue
            self.__check_destination_memo(account_resp, index, destination)

    def __check_destination_memo(
        self, account_resp: dict, index: int, destination: str
    ) -> None:
        memo_required_config_key = "config.memo_required"
        memo_required_config_value = "MQ=="
        data = account_resp["data"]
        if data.get(memo_required_config_key) == memo_required_config_value:
            raise AccountRequiresMemoError(
                "Destination account requires a memo in the transaction.",
                destination,
                index,
            )

    def __get_check_memo_required_destinations(
        self, transaction: Transaction
    ) -> Generator[Tuple[int, str], Any, Any]:
        destinations = set()
        for index, operation in enumerate(transaction.operations):
            if isinstance(
                operation,
                (
                    Payment,
                    AccountMerge,
                    PathPaymentStrictSend,
                    PathPaymentStrictReceive,
                ),
            ):
                destination: str = operation.destination
            else:
                continue
            if destination in destinations:
                continue
            destinations.add(destination)
            yield index, destination

    async def fetch_base_fee(self) -> int:
        """Fetch the base fee. Since this hits the server, if the server call fails,
        you might get an error. You should be prepared to use a default value if that happens.

        :return: the base fee
        :raises:
            :exc:`ConnectionError <stellar_sdk.exceptions.ConnectionError>`
            :exc:`NotFoundError <stellar_sdk.exceptions.NotFoundError>`
            :exc:`BadRequestError <stellar_sdk.exceptions.BadRequestError>`
            :exc:`BadResponseError <stellar_sdk.exceptions.BadResponseError>`
            :exc:`UnknownRequestError <stellar_sdk.exceptions.UnknownRequestError>`
        """
        return await self.__fetch_base_fee_async()

    async def __fetch_base_fee_async(self) -> int:
        latest_ledger = await self.ledgers().order(desc=True).limit(1).call()
        base_fee = self.__handle_base_fee(latest_ledger)
        return base_fee

    def __handle_base_fee(self, latest_ledger: dict) -> int:
        base_fee = 100
        if (
            latest_ledger["_embedded"]
            and latest_ledger["_embedded"]["records"]
            and latest_ledger["_embedded"]["records"][0]
        ):
            base_fee = int(
                latest_ledger["_embedded"]["records"][0]["base_fee_in_stroops"]
            )
        return base_fee

    async def close(self) -> None:
        """Close underlying connector.

        Release all acquired resources.
        """
        await self._client.close()

    async def __aenter__(self) -> "Server":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()  # type: ignore[misc]

    def __str__(self):
        return f"<Server [horizon_url={self.horizon_url}, client={self._client}]>"
