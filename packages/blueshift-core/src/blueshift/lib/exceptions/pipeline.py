
try:
        from zipline_pipeline.errors import (
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