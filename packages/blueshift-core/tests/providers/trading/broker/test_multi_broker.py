import pytest
from unittest.mock import MagicMock, patch
from blueshift.providers.trading.broker.multi_broker import MultiBroker
from blueshift.interfaces.trading.broker import ILiveBroker
from blueshift.lib.trades._accounts import Account
from blueshift.lib.exceptions import InsufficientFund, PriceOutOfRange

class TestMultiBroker:
    @pytest.fixture
    def mock_broker_a(self):
        b = MagicMock(spec=ILiveBroker)
        b.name = "broker_a"
        b.is_connected = True
        b.orders = {}
        b.open_orders = {}
        b.positions = {}
        b.exists.return_value = True
        return b

    @pytest.fixture
    def mock_broker_b(self):
        b = MagicMock(spec=ILiveBroker)
        b.name = "broker_b"
        b.is_connected = True
        b.orders = {}
        b.open_orders = {}
        b.positions = {}
        b.exists.return_value = True
        return b

    @pytest.fixture
    def multi_broker(self, mock_broker_a, mock_broker_b):
        brokers = {"broker_a": mock_broker_a, "broker_b": mock_broker_b}
        # Force broker_a to be primary if determined by iterator order, 
        # but better to rely on what MultiBroker picks or pass explicit order if using OrderedDict
        return MultiBroker(brokers)

    def test_init(self, multi_broker, mock_broker_a, mock_broker_b):
        assert multi_broker.name == "multi_broker"
        assert len(multi_broker.brokers) == 2
        
        # Test is_connected depends on primary
        primary_name = multi_broker._primary_broker_name
        primary = multi_broker.brokers[primary_name]
        other = mock_broker_b if primary_name == "broker_a" else mock_broker_a
        
        primary.is_connected = True
        other.is_connected = False
        assert multi_broker.is_connected is True
        
        primary.is_connected = False
        other.is_connected = True
        assert multi_broker.is_connected is False

    def test_routing_default(self, multi_broker, mock_broker_a, mock_broker_b):
        # Setup: Ensure both support the asset
        order = MagicMock()
        order.asset.symbol = "AAPL"
        
        mock_broker_a.exists.return_value = True
        mock_broker_b.exists.return_value = True
        
        # Manipulate latency: make broker_b faster
        multi_broker._latencies["broker_a"] = 0.5
        multi_broker._latencies["broker_b"] = 0.1
        
        mock_broker_b.place_order.return_value = "order_b"
        
        result_id = multi_broker.place_order(order)
        
        assert result_id == "broker_b:order_b"
        mock_broker_b.place_order.assert_called_once_with(order)
        mock_broker_a.place_order.assert_not_called()

    def test_routing_retry(self, multi_broker, mock_broker_a, mock_broker_b):
        order = MagicMock()
        order.asset.symbol = "AAPL"
        
        mock_broker_a.exists.return_value = True
        mock_broker_b.exists.return_value = True
        
        # Make broker_a the preferred one (lower latency)
        multi_broker._latencies["broker_a"] = 0.1
        multi_broker._latencies["broker_b"] = 0.5
        
        # Make broker_a fail
        mock_broker_a.place_order.side_effect = InsufficientFund("No cash")
        mock_broker_b.place_order.return_value = "order_b_retry"
        
        result_id = multi_broker.place_order(order)
        
        # Should have tried A, failed, then B
        mock_broker_a.place_order.assert_called_once()
        mock_broker_b.place_order.assert_called_once()
        assert result_id == "broker_b:order_b_retry"

    def test_routing_custom(self, mock_broker_a, mock_broker_b):
        def custom_router(order, brokers):
            return "broker_b"
            
        mb = MultiBroker({"broker_a": mock_broker_a, "broker_b": mock_broker_b}, router=custom_router)
        
        order = MagicMock()
        mock_broker_b.place_order.return_value = "order_2"
        
        result_id = mb.place_order(order)
        assert result_id == "broker_b:order_2"
        mock_broker_b.place_order.assert_called_once_with(order)

    def test_account_aggregation(self, multi_broker, mock_broker_a, mock_broker_b):
        # Create mock Account objects with attributes
        acct_a = MagicMock(spec=Account)
        acct_a.cash = 100.0
        acct_a.margin = 50.0
        acct_a.gross_exposure = 1000.0
        acct_a.net_exposure = 500.0
        acct_a.holdings = 500.0
        acct_a.cost_basis = 400.0
        acct_a.mtm = 100.0
        acct_a.commissions = 10.0
        acct_a.charges = 5.0

        acct_b = MagicMock(spec=Account)
        acct_b.cash = 200.0
        acct_b.margin = 100.0
        acct_b.gross_exposure = 2000.0
        acct_b.net_exposure = -1000.0
        acct_b.holdings = 1000.0
        acct_b.cost_basis = 900.0
        acct_b.mtm = 100.0
        acct_b.commissions = 20.0
        acct_b.charges = 10.0
        
        mock_broker_a.get_account.return_value = acct_a
        mock_broker_b.get_account.return_value = acct_b
        
        res_acct = multi_broker.get_account()
        
        assert isinstance(res_acct, Account)
        assert res_acct.cash == 300.0
        assert res_acct.margin == 150.0
        assert res_acct.gross_exposure == 3000.0
        assert res_acct.net_exposure == -500.0
        assert res_acct.commissions == 30.0

    def test_position_aggregation(self, multi_broker, mock_broker_a, mock_broker_b):
        asset_x = MagicMock()
        pos_a = MagicMock()
        pos_a.copy.return_value = pos_a
        
        pos_b = MagicMock()
        
        mock_broker_a.positions = {asset_x: pos_a}
        mock_broker_b.positions = {asset_x: pos_b}
        
        positions = multi_broker.positions
        
        assert asset_x in positions
        # Should have added pos_b to pos_a (or a copy of it)
        pos_a.add_to_position.assert_called_with(pos_b)

    def test_dataportal_delegation(self, multi_broker, mock_broker_a, mock_broker_b):
        primary_name = multi_broker._primary_broker_name
        primary = multi_broker.brokers[primary_name]
        
        multi_broker.symbol("AAPL")
        primary.symbol.assert_called_with("AAPL", None)
        
        multi_broker.current("AAPL", "close")
        primary.current.assert_called_with("AAPL", "close")

    def test_order_id_mapping(self, multi_broker, mock_broker_a):
        order = MagicMock()
        order.oid = "123"
        # Mock copy behavior
        order_copy = MagicMock()
        order_copy.oid = "123"
        
        mock_broker_a.orders = {"123": order}
        
        with patch('blueshift.providers.trading.broker.multi_broker.copy', return_value=order_copy) as mock_copy:
            orders = multi_broker.orders
            
            assert "broker_a:123" in orders
            # Verify set_order_id was called on the copy
            order_copy.set_order_id.assert_called_with("broker_a:123")

    def test_cancel_order(self, multi_broker, mock_broker_a):
        mock_broker_a.cancel_order.return_value = "123"
        
        composite_id = "broker_a:123"
        res = multi_broker.cancel_order(composite_id)
        
        assert res == composite_id
        mock_broker_a.cancel_order.assert_called_with("123")