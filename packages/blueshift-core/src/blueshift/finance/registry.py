from blueshift.interfaces.trading.simulations import (
    register_slippage_model, register_margin_model, register_cost_model, register_charge_model,
    set_builtin_slippage_model_loader, set_builtin_margin_model_loader, 
    set_builtin_charge_model_loader, set_builtin_cost_model_loader)

def install_slippage_models():
    from blueshift.finance.slippage._slippage import (
        NoSlippage, FixedSlippage, BidAskSlippage, VolumeSlippage, NSESlippage, NYSESlippage,
        FXSlippage, CryptoSlippage)
    
    register_slippage_model('no-slippage', NoSlippage)
    register_slippage_model('fixesd', FixedSlippage)
    register_slippage_model('spread', BidAskSlippage)
    register_slippage_model('bid-ask', BidAskSlippage)
    register_slippage_model('volume', VolumeSlippage)
    register_slippage_model('NSE', NSESlippage)
    register_slippage_model('NFO', NSESlippage)
    register_slippage_model('NYSE', NYSESlippage)
    register_slippage_model('FX', FXSlippage)
    register_slippage_model('CRYPTO', CryptoSlippage)

def install_margin_models():
    from blueshift.finance.margin._margins import (
        NoMargin, FlatMargin, RegTMargin, NSEMargin, FXMargin
    )
    register_margin_model('no-margin', NoMargin)
    register_margin_model('flat', FlatMargin)
    register_margin_model('regT', RegTMargin)
    register_margin_model('NSE', NSEMargin)
    register_margin_model('NFO', NSEMargin)
    register_margin_model('NYSE', RegTMargin)
    register_margin_model('FX', FXMargin)

def install_cost_models():
    from blueshift.finance.commission._brokerage import (
        NoCommission, PerDollar, PerShare, PerOrder, NSECommission, NYSECommission, FXCommission
    )

    register_cost_model('no-cost', NoCommission)
    register_cost_model('per-dollar', PerDollar)
    register_cost_model('per-unit', PerShare)
    register_cost_model('per-order', PerOrder)
    register_cost_model('NSE', NSECommission)
    register_cost_model('NFO', NSECommission)
    register_cost_model('NYSE', NYSECommission)
    register_cost_model('FX', FXCommission)

def install_charge_models():
    from blueshift.finance.commission._charges import (
        NoCharge, DollarCharge, PerShareCharge, PerOrderCharge, NSECharges, NYSECharges, FXCharges
    )

    register_charge_model('no-charge', NoCharge)
    register_charge_model('per-dollar', DollarCharge)
    register_charge_model('per-unit', PerShareCharge)
    register_charge_model('per-order', PerOrderCharge)
    register_charge_model('NSE', NSECharges)
    register_charge_model('NYSE', NYSECharges)
    register_charge_model('FX', FXCharges)


set_builtin_slippage_model_loader(install_slippage_models)
set_builtin_margin_model_loader(install_margin_models)
set_builtin_cost_model_loader(install_cost_models)
set_builtin_charge_model_loader(install_charge_models)