from __future__ import annotations
from .exceptions import *
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    try:
        from .pipeline import (
            NoFurtherDataError,
            WindowLengthNotPositive,
            UnsupportedDataType,
            NonExistentAssetInTimeFrame,
            UnsupportedPipelineOutput,
            DTypeNotSpecified,
            InvalidOutputName,
            NonSliceableTerm,
            NonWindowSafeInput,
            NotDType,
            NonPipelineInputs,
            TermInputsNotSpecified,
            TermOutputsEmpty,
            UnsupportedDType,
            WindowLengthNotSpecified,
            BadPercentileBounds,
            UnknownRankMethod,
            IncompatibleTerms,
        )
    except ImportError:
        pass

_pipline_errors = None
def __getattr__(name):
    # Lazy-load everything from pipeline errors only if needed
    global _pipline_errors
    if name in _lazy_b_exports():
        if _pipline_errors is None:
            from . import pipeline
            _pipline_errors = pipeline

        attr = getattr(_pipline_errors, name)
        if not attr:
            raise AttributeError(f"module {__name__} has no attribute {name}")
        else:
            globals()[name] = attr
        return globals()[name]
    raise AttributeError(f"module {__name__} has no attribute {name}")

def _lazy_b_exports():
    return {
        'NoFurtherDataError',
        'WindowLengthNotPositive',
        'UnsupportedDataType',
        'NonExistentAssetInTimeFrame',
        'UnsupportedPipelineOutput',
        'DTypeNotSpecified',
        'InvalidOutputName',
        'NonSliceableTerm',
        'NonWindowSafeInput',
        'NotDType',
        'NonPipelineInputs',
        'TermInputsNotSpecified',
        'TermOutputsEmpty',
        'UnsupportedDType',
        'WindowLengthNotSpecified',
        'BadPercentileBounds',
        'UnknownRankMethod',
        'IncompatibleTerms',
    }
