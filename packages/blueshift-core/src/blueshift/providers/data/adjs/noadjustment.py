from blueshift.interfaces.data.adjustments import AdjustmentHandler, register_adj_handler

class NoAdjustments(AdjustmentHandler):
    """ a dummy adjustment handler. """
    def initialize(self, *args, **kwargs):
        pass
    
    def read(self, *args, **kwargs):
        pass
    
    def write(self, *args, **kwargs):
        pass
    
    def rename(self, *args, **kwargs):
        pass
        
    def refresh(self, *args, **kwargs):
        pass
    
    def dividend_for_assets(self, *args, **kwargs):
        return {}
    
    def split_for_assets(self, *args, **kwargs):
        return {}
    
    def merger_for_assets(self, *args, **kwargs):
        return {}
    
    def adjs_for_assets(self, *args, **kwargs):
        return {}
    
    def load_adjustments(self,
                         dates,
                         assets,
                         should_include_splits,
                         should_include_mergers,
                         should_include_dividends,
                         adjustment_type):
        if adjustment_type == 'price':
            return {'price':{}}
        elif adjustment_type=='volume':
            return {'volume':{}}
        else:
            return {'price':{}, 'volume':{}}
        
register_adj_handler('no-adj', NoAdjustments)