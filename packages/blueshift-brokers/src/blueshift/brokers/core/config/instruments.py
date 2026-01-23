from __future__ import annotations
from typing import Any, TYPE_CHECKING
from dataclasses import dataclass, field

from blueshift.lib.exceptions import ValidationError

from .resolver import (
    CompiledExpr, ComputedFields, ConfigRegistry, get_fields, 
    CustomCallable)

if TYPE_CHECKING:
    import pandas as pd
else:
    import blueshift.lib.common.lazy_pandas as pd

@dataclass(frozen=True, slots=True)
class AssetDef:
    """ Definition for extracting a specific asset type from master data """
    file_match: str|None
    filters: list[CompiledExpr]
    asset_class:str
    mapping: ComputedFields|None
    details: ComputedFields|None
    custom: CustomCallable|None
    vectorized: bool

    @classmethod
    def from_config(cls, spec: dict[str, Any], registry:ConfigRegistry) -> AssetDef:
        file_match = spec.get('file_match')
        asset_class = spec.get('asset_class','')
        
        filters = []
        if 'filter' in spec:
            filter_spec = spec['filter']
            if isinstance(filter_spec, str):
                filters.append(CompiledExpr(filter_spec, registry))
            elif isinstance(filter_spec, list):
                for f in filter_spec:
                    filters.append(CompiledExpr(f, registry))
        
        custom = None
        if 'custom' in spec:
            custom_ref = spec['custom']
            if callable(custom_ref):
                 custom = CustomCallable(custom_ref)
            elif isinstance(custom_ref, str):
                if custom_ref in registry and callable(registry[custom_ref]):
                    custom = CustomCallable(registry[custom_ref])
                else:
                    raise ValidationError(f"Custom function {custom_ref} not found in registry")

        mapping = None
        if 'mapping' in spec:
            mapping = ComputedFields(get_fields(spec['mapping'], registry))
            
        if not mapping and not custom:
            raise ValidationError(f'one of mapping or custom function is required')
        
        details = None
        if 'details' in spec:
            details = ComputedFields(get_fields(spec['details'], registry))
        
        vectorized = spec.get('vectorized', False)
        
        return cls(file_match, filters, asset_class, mapping, details, custom, vectorized)

    def _resolve_element(self, row: dict[str, Any], **context) -> dict[str, Any] | None:
        """ 
        Process a single row/item of data.
        Returns mapped fields if the item matches the filter (if any), else None.
        """
        ctx = context.copy()
        ctx['row'] = row # standard context for row-based operations
        ctx['item'] = row # alias for list-based operations
        
        if self.filters:
            for filter_expr in self.filters:
                if not filter_expr.resolve(**ctx):
                    return None
                
        details:dict[str, Any] = {}
        if self.details:
            details = self.details.resolve(**ctx)
        
        if self.custom:
            ctx['details'] = details
            return self.custom.resolve(**ctx)
            
        mappings:dict[str, Any] = {}
        if self.mapping:
            mappings = self.mapping.resolve(**ctx)
            if not mappings:
                return None

        mappings['details'] = details
        if self.asset_class and 'asset_class' not in mappings:
            mappings['asset_class'] = self.asset_class
            
        return mappings

    def _resolve_vectorized(self, data: pd.DataFrame|list[dict[str,Any]], **context) -> list[dict]:
        """
        Process a dataset (list of items or pandas DataFrame) in vectorized mode.
        """
        if not isinstance(data, pd.DataFrame):
            try:
                data = pd.DataFrame(data)
            except Exception as e:
                raise ValidationError(f'expected a dataframe, or convertible to one: {str(e)}')
        else:
            data = data.copy()

        ctx = context.copy()
        ctx['data'] = data
        filtered_data = data
        
        # Vectorized Filtering
        if self.filters:
            mask = None
            for filter_expr in self.filters:
                res = filter_expr.resolve(**ctx)
                if mask is None:
                    mask = res
                else:
                    mask = mask & res
            
            if mask is not None:
                if isinstance(mask, bool): # Scalar result
                    if not mask:
                        filtered_data = pd.DataFrame(columns=data.columns)
                else:
                    filtered_data = data[mask]
            
            # Update context with filtered data for mapping
            ctx['row'] = filtered_data
            ctx['item'] = filtered_data
            ctx['data'] = filtered_data

        details = {}
        if self.details:
            details = self.details.resolve(**ctx)

        # custom mapping
        if self.custom:
            ctx['details'] = details
            return self.custom.resolve(filtered_data, **ctx)

        # Vectorized Mapping -> each mapping should return a pd.Series
        if self.mapping:
            mapped_data = self.mapping.resolve(**ctx)
            filtered_data = pd.DataFrame(mapped_data)

        if self.asset_class and 'asset_class' not in filtered_data.columns:
            filtered_data['asset_class'] = self.asset_class

        filtered_data = filtered_data.drop_duplicates()

        if details:
            details = pd.DataFrame(details, index=filtered_data.index)
            filtered_data["details"] =pd.DataFrame(details).apply(lambda row: row.to_dict(), axis=1)
        
        return filtered_data.to_dict(orient="records")

    def resolve(self, data: list[dict[str,Any]]|pd.DataFrame, **context) -> list[dict[str,Any]]:
        """
        Main entry point for resolving asset data.
        Dispatches to vectorized or element-wise processing based on configuration.
        """
        if self.vectorized:
            return self._resolve_vectorized(data, **context)
        
        if isinstance(data, pd.DataFrame):
            results:list[dict[str, Any]] = []
            # Iterate over DataFrame rows as dicts
            for _, row in data.iterrows():
                res = self._resolve_element(row.to_dict(), **context)
                if res is not None:
                    results.append(res)
            return results
        
        if isinstance(data, list):
            results = []
            for item in data:
                res = self._resolve_element(item, **context)
                if res is not None:
                    results.append(res)
            return results
        
        raise ValidationError(f'expected a datafrmae or list of dicts')



