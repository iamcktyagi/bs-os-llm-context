from __future__ import annotations
from typing import Any, Type, Callable
from enum import Enum
from copy import deepcopy
from jsonschema import Draft202012Validator
from jsonschema.protocols import Validator
from jsonschema.validators import extend
from jsonschema.exceptions import ValidationError as SchemaValidationError

from .resolver import ConfigRegistry

from blueshift.lib.common.constants import OHLCV_COLUMNS
from blueshift.lib.common.enums import BlotterType, AlgoMode, ExecutionMode
from blueshift.interfaces.trading.rms import NoRMS
from blueshift.lib.exceptions import ValidationError

_MAX_TICKERS = 25
_MAX_SUBSCRIPTION = 100
_MAX_NBARS = 1000
_MAX_RETRY = 3
_RATE_LIMIT = 200
_RATE_LIMIT_PERIOD = 100
_MAX_DATA_FETCH = 1000
_MAX_PAGE_FETCH = 50
_API_DELAY_PROTECTION = 0.01

COMMON_DEFS = {
    "$defs": {
        # an expr (expression) is pre-compiled and eval-ed at runtime with the context as locals.
        # must return a value or a boolean.
        "expr": {
            "type": "string", # the expression to eval
            "description": "A python expression that is evaluated in the current context",
        },
        # a python function that is called at runtime with the context as keyword arguments, must 
        # return a value or a boolean.
        "callable_ref": {
            "type": "string", # the name of the function f(**context) to run with the context
            "description": "Python function reference (e.g. registry name or module:function) called with current context as keyword arguments",
        },
        # an error rule that checks the `condition` and if True, raises the given exception class 
        # with the error messaged. The error message is `format`-ed with the current context.
        "error_rule": {
            "type": "object",
            "description": "error condition and exceptions to raise. If condition is missing, never raised",
            "required": ["condition", "exception", "message"],
            "properties": {
                "condition": {"$ref": "#/$defs/expr"},
                "exception": {"type": "string"},  # e.g. "ServerError" / "OrderError"
                "message": {"type": "string"}, # error message to raise exception with
            },
            "additionalProperties": False,
        },
        # a computed field can either be defined by a custom function or by a source (an expression).
        # If a condition is given and is False, a NULL sentinel is returned. If a `transform` function
        # is specified, it is invoked with the returned value from the `source` expression as the first 
        # argument and the context as keyword arguments. 
        "field_spec": {
            "type": "object",
            "description": "A computed field from a given source, with optional transformation, or a custom callable. If condition is present and false, returns NULL sentinel",
            "required":["source"],
            "properties": {
                "source":  {"$ref": "#/$defs/expr"}, # source expression to eval with current context
                "condition":  {"$ref": "#/$defs/expr"}, # condition expression to eval with current context
                "transform": {"$ref": "#/$defs/callable_ref"}, # transform function called with current context
                "custom":{"$ref": "#/$defs/callable_ref"}, # custom function called with current context, everything else is ignored
                "default_value": {}
            },
            "additionalProperties": False,
        },
        # a dict with string keys and computed fields as values.
        "fields_object": {
            "type": "object",
            "description": "A dict of field definitions. If any field returns NULL sentinel, that field is skipped",
            "additionalProperties": {"$ref": "#/$defs/field_spec"},
        },
        # a dict with string keys and computed fields as values, wrapped inside a dict with 'fields` key.
        "fields_group": {
            "type": "object",
            "description": "A dict of field definitions like {\"fields\":{...}}",
            "properties": {
                "custom": {"$ref": "#/$defs/callable_ref"}, # custom callable
                "fields": {"$ref": "#/$defs/fields_object"} # generate fields by hand
                },
            "additionalProperties": False
        },
        # a dict with str key and computed fields as values that should produce a tuple of 
        # (asset:MarketData, timestamp:pd.Timestamp, data:dict). This is as expected output 
        # from streaming data, before it is passed on to blueshift for processing.
        "streaming_data_fields_group":{
            "type": "object",
            "description": "Tuple-like output: (asset, timestamp, data)",
            "properties": {
                "asset": {"$ref": "#/$defs/field_spec"}, # returns the Asset object
                "timestamp": {"$ref": "#/$defs/field_spec"}, # returns the timestamp
                "data":{"$ref": "#/$defs/fields_object"}, # return the price data
                # Or alternatively use a custom function that returns asset, timestamp. data tuple
                "custom": {"$ref": "#/$defs/callable_ref"},
            }
        },
        # allows parsing of data returned from an API response and convert this to a pandas DataFrame.
        # the `frame` type determines how to interpret the returned data. The `rename` mapping 
        # maps the OHLCV columns to column names (or column index) in the returned data. The `timestamp`
        # enables identifying the timestamp index column and parse that into a pandas DatetimeIndex.
        # Alternatively, pass a `custom` function that takes the data returned as the first argument 
        # and the context as keyword arguments and must return a pandas DataFrame.
        "ohlc_fields_group":{
            "type": "object",
            "description": "OHLC/ OHLCV or OHLCVX data conversion rule",
            "properties": {
                # if multiple symbols, where to find the symbols, "key" -> top level keys are the 
                # symbols, "data" -> part of the data record. Omit this for single ticker data
                # that does not include ticker in the returned data
                "symbol":{"type": "string", "enum": ["key", "data"]},
                # record -> a list of dicts with columns as keys, dict_of_arrays -> a dict of lists with columns as keys, values -> list 
                "frame":{"type": "string", "enum": ["records", "dict_of_arrays", "values"]},
                # map the timestamp + OHLCV col to provided column names. In case only the values are  
                # provided without any column names, use this to specify the mapping to column numbers
                # e.g. {"timestamp":0,"open":1, "high":2, "low":3, "close":4} etc.
                "rename": {
                    "type": "object",
                    "description": "OHLCV col to input col names mapping.", # simple mapping
                    "additionalProperties": {"type": "string"}
                    },
                # logic to parse the timestamp index column. The column name must be 'timestamp'
                "timestamp": {
                    "type":"object",
                    "description": "how to convert the timestamp columns to pd.DatetimeIndex",
                    "unit": {"type": "string", "enum": ["s", "ms", "us", "ns"]}, # in case numbers
                    "format": {"type": "string"}, # in case string-like and needs a format string
                    },
                # Or alternatively use a custom function
                "custom": {"$ref": "#/$defs/callable_ref"},
            }
        },
        # data structure for capturing quotes and market depth information (class `MarketDepth`).
        "quote_fields_group": {
            "type": "object",
            "description": "Market depth and quote data conversion rule (see class MarketDepth in the core package)",
            "properties": {
                # use a custom function that returns marketdepth structure
                "custom": {"$ref": "#/$defs/callable_ref"},
                # or define by hand
                "timestamp": {"$ref": "#/$defs/field_spec"},
                "last": {"$ref": "#/$defs/field_spec"},
                "close": {"$ref": "#/$defs/field_spec"},
                "bid": {"$ref": "#/$defs/field_spec"},
                "bid_volume": {"$ref": "#/$defs/field_spec"},
                "ask": {"$ref": "#/$defs/field_spec"},
                "ask_volume": {"$ref": "#/$defs/field_spec"},
                "open": {"$ref": "#/$defs/field_spec"},
                "high": {"$ref": "#/$defs/field_spec"},
                "low": {"$ref": "#/$defs/field_spec"},
                "upper_circuit": {"$ref": "#/$defs/field_spec"},
                "lower_circuit": {"$ref": "#/$defs/field_spec"},
                "market_depth": {
                    "type": "object",
                    "description": "L2 market depth",
                    "properties": {
                        "bids":{
                            "type":"array",
                            "items":{
                                "type": "array",
                                "prefixItems":[
                                    { "$ref": "#/$defs/field_spec" }, # the bid price
                                    { "$ref": "#/$defs/field_spec" }, # the bid size
                                ],
                                "minItems": 2,
                                "maxItems": 2
                            },
                        },
                        "asks":{
                            "type":"array",
                            "items":{
                                "type": "array",
                                "prefixItems":[
                                    { "$ref": "#/$defs/field_spec" }, # the ask price
                                    { "$ref": "#/$defs/field_spec" }, # the ask size
                                ],
                                "minItems": 2,
                                "maxItems": 2
                            },
                        },
                    },
                    "additionalProperties": False
                }
            },
            "additionalProperties": True
        },
        # helper for enum mapping
        "mapping_target": {
            "description": "a single or multiple values for an enum target",
            "oneOf": [
                {"type": "string", "minLength": 1},
                {"type": "array", "minItems": 1, "items": {"type": "string", "minLength": 1}, "uniqueItems": True},
            ]
        },
        # enum mapping from Blueshift enums (like order type, order validity, product type etc.). The 
        # core part is the mapping with an optional default. Additionally from and to blueshift conversion
        # functions to register for direct use (for e.g. in field objects or in conversion pipelines)
        "enum_map":{
            "type": "object",
            "description": "A conversion mapping with optional automatic converter functions generation",
            "properties": {
                "map": {
                    "type": "object",
                    "additionalProperties": {"$ref": "#/$defs/mapping_target"},
                },
                "default_value":{"type": "string"},
                "from_blueshift": {"type": "string"},
                "to_blueshift": {"type": "string"},
            }
        },
        # conversion logic definition from input data to blueshift objects (like Order, Position
        # Account, etc.). If condition specified and is False a sentinel NULL is returned, so that
        # conversion pipeline can continue trying the next logic.
        "object_conversion": {
            "type": "object",
            "description": "Convert data from API or streaming to blueshift object",
            "properties": {
                "condition": {"$ref": "#/$defs/expr"}, # optional condition to skip this rule
                "custom": {"$ref": "#/$defs/callable_ref"}, # python function, will ignore rest
                "fields": {"$ref": "#/$defs/fields_object"}, # field conversion rules
            },

            "allOf": [
                {
                    "if": {"required": ["custom"]},
                    "then": {
                        "required": ["custom"],
                        "not": {"required": ["fields"]}
                    },
                    "else": {
                        "required": ["fields"],
                        "not": {"required": ["custom"]}
                    },
                }
            ],
        },
        # a collection of object conversion rules to be tried in order.
        "conversion_pipeline": {
            "type": "array",
            "description": "A list of conversion rules to be applied in order (and return on first success)",
            "minItems": 1,
            "items": {"$ref": "#/$defs/object_conversion"},
        },
        # the conversion rule for quotes streaming data
        "streaming_quote_object_conversion": {
            "type": "object",
            "description": "Convert quote data from streaming to blueshift object",
            "properties": {
                "condition": {"$ref": "#/$defs/expr"}, # optional condition to skip this rule
                "custom": {"$ref": "#/$defs/callable_ref"}, # python function, will ignore rest
                "fields": {"$ref": "#/$defs/quote_fields_group"}, # field conversion rules
            },
        },
        # pipeline for streaming quotes
        "streaming_quote_conversion_pipeline": {
            "type": "array",
            "description": "A list of conversion rules to be applied in order (and return on first success)",
            "minItems": 1,
            "items": {"$ref": "#/$defs/streaming_quote_object_conversion"},
        },
        # the conversion rule for streaming data, specifies the structure of the expected output.
        "streaming_data_object_conversion": {
            "type": "object",
            "description": "Convert price data from streaming to blueshift object",
            "properties": {
                "condition": {"$ref": "#/$defs/expr"}, # optional condition to skip this rule
                "custom": {"$ref": "#/$defs/callable_ref"}, # python function, will ignore rest
                "fields": {"$ref": "#/$defs/streaming_data_fields_group"}, # field conversion rules
            },

            "allOf": [
                {
                    "if": {"required": ["custom"]},
                    "then": {
                        "required": ["custom"],
                        "not": {"required": ["fields"]}
                    },
                    "else": {
                        "required": ["fields"],
                        "not": {"required": ["custom"]}
                    },
                }
            ],
        },
        # a conversion pipeline (collection of conversion rules) for processing streaming inputs
        "streaming_data_conversion_pipeline": {
            "type": "array",
            "description": "A list of conversion rules to be applied in order (and return on first success)",
            "minItems": 1,
            "items": {"$ref": "#/$defs/streaming_data_object_conversion"},
        },
        # definition for parsing specifc instrument from the master data. asset_type maps to 
        # internal blueshift Asset class. Filters specifies a series of filters to apply. File 
        # match is the specific file to read from (in case of an archive) or append to master_data
        # endpoint url if it is not a compressed archive. Finally mapping defines the column 
        # transformation to pass on to the asset factory function of Blueshift.
        "instrument_def": {
            "type": "object",
            "description": "Definition for extracting a specific asset type from master data",
            "properties": {
                "file_match": {"type": "string"},
                "filter": {
                    "oneOf": [
                        {"$ref": "#/$defs/expr"},
                        {"type": "array", "items": {"$ref": "#/$defs/expr"}}
                    ]
                },
                "asset_class": {
                    "type": "string", 
                    "enum": ["mktdata", "equity", "equity-futures", "equity-options", "crypto","fx"]
                },
                "mapping": {"$ref": "#/$defs/fields_object"}, # passed on as kwargs to asset factory
                "details": {"$ref": "#/$defs/fields_object"}, # passed on as details dict
                "custom": {"$ref": "#/$defs/callable_ref"},
                "vectorized": {"type": "boolean", "default": False}
            },
            "oneOf": [
                {"required": ["mapping"]},
                {"required": ["custom"]}
            ],
            "additionalProperties": False
        },
        # definition for parsing master data and creating the instrument list.
        "master_data_def": {
            "type": "object",
            "description": "Configuration for fetching and parsing instrument master data",
            "required": ["endpoint", "assets"],
            "properties": {
                "preload":{"type": "boolean", "default": True},
                "endpoint": {"$ref": "#/$defs/endpoint_def"},
                "mode": {"type": "string", "enum": ["list", "file"], "default": "list"},
                "format": {"type": "string", "enum": ["json", "csv", "txt"], "default": "csv"},
                "compression": {"type": "string", "enum": ["zip", "gzip", "none"], "default": "none"},
                "csv_options": {"type": "object"},
                "assets": {
                    "type": "array",
                    "items": {"$ref": "#/$defs/instrument_def"},
                    "minItems": 1
                }
            },
            "additionalProperties": False
        },
    }
}

BROKER_SCHEMA = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    **deepcopy(COMMON_DEFS),
    "type": "object",
    "additionalProperties": True,
    "required":["name","calendar","credentials"],
    "properties": {
        "name": {"type": "string"},
        "variant": {"type": "string"},
        "display_name": {"type": "string","description":"defaults to `name`"},
        "version": {"type": "string","default": "0.0.0"},
        "description": {
            "type": "string",
            "description": "defaults to `Blueshift Broker implementation for name`."
        },
        "calendar": {
            "type": "string", 
            "description": "Trading calendar identifier (e.g., 'NSE', 'NYSE', 'NSE_UAT')"
        },
        # the fields for credentials
        "credentials": {
            "type":"object",
            "required":["fields"],
            **deepcopy(COMMON_DEFS),
            "fields":{
                "type": "array",
                "items": {"type": "string"},
                "uniqueItems": True,
                "minItems": 1
            },
            "setters":{
                "type": "object",
                "additionalProperties": {"$ref": "#/$defs/field_spec"},
            },
            "validator":{"$ref": "#/$defs/expr"},
        },
        # broker specific options
        "options":{
            "type": "object",
            "default": {},
            **deepcopy(COMMON_DEFS),
            "properties": {
                "agent": {"type": "string", "default": "blueshift"},
                "timeout": {"type": "number", "default": 5},
                "fractional_trading": {"type": "boolean", "default": False},
                "lazy_session": {"type": "boolean", "default": False},
                "check_host_before_trade": {"type": "boolean", "default": True},
                "unwind_before_trade": {"type": "boolean", "default": False},
                "intraday_cutoff": {"type": "array", "default": [15, 20]},
                "ccy": {"type": "string", "default": "LOCAL"},
                "blotter_type": {"default": BlotterType.VIRTUAL},
                "data_columns": {"type": "array", "default": OHLCV_COLUMNS},
                # expiry dispatch in the form of {exchange_name:dispatcher_name}. The dispatcher
                # will be fetched by get_expiry_dispatch(dispatcher_name)
                "expiry_dispatch": {
                    "type":"object",
                    "additionalProperties": {"type": "string"},
                    "default": {},
                },
                "rms": {
                    "type":"object",
                    "properties":{
                        "rms":{"type":"string"},
                        "additionalProperties": True
                    },
                    "default":{
                        "rms":"no-rms",
                    },
                },
                "interface":{
                    "type":"string", 
                    "enum":["broker","trader","data-portal"],
                    "default":"broker",
                    },
                "stale_data_tolerance": {"type": "number", "default": 5},
                "supported_modes": {"type": "array", "default": [AlgoMode.LIVE, AlgoMode.EXECUTION]},
                "execution_modes": {"type": "array", "default": [ExecutionMode.AUTO]},
                "max_tickers": {"type": "number", "default": _MAX_TICKERS}, # override in data api endpoint
                "max_subscription": {"type": "number", "default": _MAX_SUBSCRIPTION}, # override in streaming
                "multi_assets_data_query": {"type": "boolean", "default": False}, # override in data api endpoint
                "multi_assets_data_subscribe": {"type": "boolean", "default": False}, # override in streaming
                "max_nbars": {"type": "number", "default": _MAX_NBARS}, # override in data api endpoint
                "max_data_fetch": {"type": "number", "default": _MAX_DATA_FETCH}, # override in data api endpoint
                "max_page_fetch": {"type": "number", "default": _MAX_PAGE_FETCH}, # override in data api endpoint
            }
        },
        # the asset classes supported
        "assets": {
            "type": "array",
            "items": {
                "type": "string",
                "enum": ["equity", "equity-futures", "equity-options", "crypto","fx"]
            },
            "default":["equity"],
            "uniqueItems": True,
            "minItems": 1
        },
        # the enum mapping from and to blueshift for convenience
        "data_frequencies": {
            "$ref": "#/$defs/enum_map",
            "default":{
                "map":{
                    '1m':'Min',
                    '1d':'Day',
                    }
                }
            },
        "order_type": {"$ref": "#/$defs/enum_map"},
        "order_validity": {"$ref": "#/$defs/enum_map"},
        "product_type": {
            "$ref": "#/$defs/enum_map",
            "default":{
                "default_value":"ProductType.DELIVERY"
                }
            },
    }
}


API_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    **deepcopy(COMMON_DEFS),
    "type": "object",
    "properties": {
        "base_url": {"type": "string"},
        "timeout": {"type": "number", "default": 60},
        "verify_cert": {"type": "boolean", "default": True},
        "proxies": {"type": "object", "default": {}},
        "max_retries": {"type": "number", "default": _MAX_RETRY},
        "rate_limit": {"type": "number", "default": _RATE_LIMIT},
        "rate_limit_period": {"type": "number", "default": _RATE_LIMIT_PERIOD},
        # recommended minimum time between two successive API calls for a given endpoint
        "api_delay_protection": {"type": "number", "default": _API_DELAY_PROTECTION},
        "extras":{
            "type":"object",
        },
        "headers": {"$ref": "#/$defs/fields_group"},
        "hooks": {"$ref": "#/$defs/hooks"},
        "errors": {
            "type": "array",
            "items": {"$ref": "#/$defs/error_rule"},
        },
        "master_data": {
            "type": "array",
            "items": {"$ref": "#/$defs/master_data_def"},
            "minItems": 1
        },
        "endpoints":{
            "type": "object",
            "properties": {
                "session": {"$ref": "#/$defs/object_endpoint_def"},
                "get_orders": {"$ref": "#/$defs/list_or_object_endpoint_def"},
                "get_order": {"$ref": "#/$defs/object_endpoint_def"},
                "place_order": {"$ref": "#/$defs/object_endpoint_def"},
                "cancel_order": {"$ref": "#/$defs/object_endpoint_def"},
                "update_order": {"$ref": "#/$defs/object_endpoint_def"},
                "get_history": {"$ref": "#/$defs/data_endpoint_def"},
                "get_account": {"$ref": "#/$defs/list_or_object_endpoint_def"},
                "get_positions": {"$ref": "#/$defs/list_or_object_endpoint_def"},
                "get_margins": {"$ref": "#/$defs/object_endpoint_def"},
                "get_charges": {"$ref": "#/$defs/object_endpoint_def"},
                "get_quote": {"$ref": "#/$defs/quote_endpoint_def"},
                "get_history_multi": {"$ref": "#/$defs/data_endpoint_def"},
            },
            "additionalProperties": {"$ref": "#/$defs/endpoint_def"},
        },
        "extras":{
            "type":"object",
            "additionalProperties": True,
        }
    },
    "required": ["base_url"],
    "additionalProperties": False,
    "$defs": {
        **deepcopy(COMMON_DEFS["$defs"]),
        "hooks": {
            "type": "object",
            "description": "A python function reference (e.g. registry name or module:function)",
            "properties": {
                # before send hook will get the relevant context, an additionally the resolved details
                # from the request - 'url', 'verb', 'headers', 'query', 'body' and 'json', and it must 
                # return them, with any updates required (for e.g. additional headers fields or params).
                "before_send": {"$ref": "#/$defs/callable_ref"},
                # after response hook will get the relevant context, including the 'status_code' and 'resp'
                # and can potentially change them in place for further processing by error handlers and 
                # the response declaration.
                "after_response": {"$ref": "#/$defs/callable_ref"},
            },
            "additionalProperties": False,
        },
        "request_def": {
            "type": "object",
            "properties": {
                "custom":{"$ref":"#/$defs/callable_ref"},
                "path":{"$ref": "#/$defs/fields_group"},
                "query":{"$ref": "#/$defs/fields_group"},
                "body":{"$ref": "#/$defs/fields_group"},
                "json":{"$ref": "#/$defs/fields_group"},
                "next_page_token":{
                    "type":"object",
                    "parameter":{"type","string"},
                    "location":{"type": "string", "enum": ["query", "body"], "default": "query"},
                },
                "headers": {"$ref": "#/$defs/fields_group"},
            },
            "additionalProperties": True,
        },
        "response_def": {
            "type": "object",
            "properties": {
                "custom":{"$ref":"#/$defs/callable_ref"},
                "payload_path": {"$ref": "#/$defs/expr"},
                "payload_type": {"type": "string", "enum": ["object", "array", "data", "quote", "custom"], "default": "object"},
                "result": {"$ref": "#/$defs/fields_group"}, # item-wise parsing logic
                "data": {"$ref": "#/$defs/ohlc_fields_group"}, # parsing logic for OHLCV data
                "quote": {"$ref": "#/$defs/quote_fields_group"}, # parsing logic for quote data
                "next_page_token":{"$ref": "#/$defs/expr"},
            },
            "additionalProperties": True,
            "allOf": [
                {
                "if": {"required": ["payload_type"],"properties": {"payload_type": {"const": "data"}}},
                "then": {"required": ["data"]}
                },
                {
                "if": {"required": ["payload_type"],"properties": {"payload_type": {"const": "quote"}}},
                "then": {"required": ["quote"]}
                },
                {
                "if": {"required": ["payload_type"],"properties": {"payload_type": {"const": "object"}}},
                "then": {"required": ["result"]}
                },
                {
                "if": {"required": ["payload_type"],"properties": {"payload_type": {"const": "array"}}},
                "then": {"required": ["items"]}
                },
                {
                "if": {"required": ["payload_type"],"properties": {"payload_type": {"const": "custom"}}},
                "then": {"required": ["custom"]}
                },
            ]
        },
        "endpoint_def": {
            "type": "object",
            "properties": {
                "custom": {"$ref": "#/$defs/callable_ref"},
                "endpoint": {"type": "string"},
                "method": {
                    "type": "string",
                    "enum": ["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"],
                },
                "request": {"$ref": "#/$defs/request_def"},
                "response": {"$ref": "#/$defs/response_def"},
                "errors": {
                    "type": "array",
                    "items": {"$ref": "#/$defs/error_rule"},
                },
                "hooks": {"$ref": "#/$defs/hooks"},
                "headers": {"$ref": "#/$defs/fields_group"},
                "use_global_headers": {"type": "boolean", "default": True},
            },
            # pass on endpoint specific timeout, rate limit details and min call delay
            "additionalProperties": True,

            # either "custom" is present (everything else is ignored, including hooks and errors)
            # or ("endpoint" and "method") are present
            "if": {"required": ["custom"]},
            "then": {
                "required": ["custom"],
                },
            "else": {
                "required": ["endpoint", "method"],
                },
        },
        "data_endpoint_def": {
             "allOf": [{"$ref": "#/$defs/endpoint_def"}],
             "properties": {
                 "response": {
                     "properties": {
                         #"payload_type": {"const": "data"}
                         "payload_type": {"enum": ["data","custom"]}
                         
                     },
                     "required": ["payload_type"]
                 }
             }
        },
        "quote_endpoint_def": {
             "allOf": [{"$ref": "#/$defs/endpoint_def"}],
             "properties": {
                 "response": {
                     "properties": {
                         #"payload_type": {"const": "quote"}
                         "payload_type": {"enum": ["quote","custom"]}
                     },
                     "required": ["payload_type"]
                 }
             }
        },
        "array_endpoint_def": {
             "allOf": [{"$ref": "#/$defs/endpoint_def"}],
             "properties": {
                 "response": {
                     "properties": {
                         #"payload_type": {"const": "array"}
                         "payload_type": {"enum": ["array","custom"]}
                     },
                     "required": ["payload_type"]
                 }
             }
        },
        "list_or_object_endpoint_def": {
             "allOf": [{"$ref": "#/$defs/endpoint_def"}],
             "properties": {
                 "response": {
                     "properties": {
                         "payload_type": {"enum": ["array", "object","custom"]}
                     }
                 }
             }
        },
        "object_endpoint_def": {
             "allOf": [{"$ref": "#/$defs/endpoint_def"}],
             "properties": {
                 "response": {
                     "properties": {
                         "payload_type": {"enum": ["custom", "object"]}
                     }
                     # payload_type is default "object", so strictly not required, but good to have constraint if explicit
                 }
             }
        },
    },
}

OBJECTS_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    **deepcopy(COMMON_DEFS),
    "type": "object",
    #"required": ["order", "position","account"],
    "properties": {
        "asset": {"$ref": "#/$defs/conversion_pipeline"}, # -> create_asset
        "order": {"$ref": "#/$defs/conversion_pipeline"}, # -> create_order
        "position": {"$ref": "#/$defs/conversion_pipeline"}, # -> create_position
        "account": {"$ref": "#/$defs/conversion_pipeline"} # -> create_account
    },
    "additionalProperties": True,
}

STREAMING_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    **deepcopy(COMMON_DEFS),
    "type": "object",
    "required": ["connections"],
    "properties": {
        "connections": {
            "type": "object",
            "minProperties": 1,
            "additionalProperties": {"$ref": "#/$defs/stream_connection"},
        },
    },
    "additionalProperties": False,

    "$defs": {
        **deepcopy(COMMON_DEFS["$defs"]),
        "channel": {
            "type": "string",
            "enum": ["data", "quote", "order", "position", "account", "news", "trade", "heartbeat", "info", "unknown"],
        },
        "streams": {
            "type": "array",
            "minItems": 1,
            "items": {"$ref": "#/$defs/channel"},
            "uniqueItems": True,
        },
        "events":{"type": "object", "additionalProperties": {"type": "string"}},
        "auth": {
            "type": "object",
            "required": ["mode"],
            "properties": {
                "mode": {"type": "string", "enum": ["url", "headers", "first_message", "none"]},
                # url mode: e.g. add query params or format tokens
                "url": {
                    "type": "object",
                    "properties": {
                        "query": {"$ref": "#/$defs/fields_group"},
                        "format": {"type": "object", "additionalProperties": {"type": "string"}},
                    },
                    "additionalProperties": False,
                },
                # headers mode
                "headers": {"$ref": "#/$defs/fields_group"},
                # first_message mode (send after connect)
                "first_message": {"$ref": "#/$defs/message_def"},
            },
            "additionalProperties": False,

            "allOf": [
                {
                    "if": {"properties": {"mode": {"const": "url"}}},
                    "then": {"required": ["url"]},
                },
                {
                    "if": {"properties": {"mode": {"const": "headers"}}},
                    "then": {"required": ["headers"]},
                },
                {
                    "if": {"properties": {"mode": {"const": "first_message"}}},
                    "then": {"required": ["first_message"]},
                }
            ],
        },

        # ----------------------------
        # Message definition (subscribe/unsubscribe/auth-first-message)
        # Supports JSON or text templates
        # ----------------------------
        "message_def": {
            "type": "object",
            "properties": {
                # the type of expected data
                "format": {"type": "string", "enum": ["json", "text", "binary"]},
                # json: build dict via groups like body/json/params/query (reusing fields_group)
                "json": {"$ref": "#/$defs/fields_group"},

                # text: format string + optional tokens fields
                "text": {"type": "string"},
                "tokens": {"$ref": "#/$defs/fields_group"},

                # binary: custom encoder hook
                "encoder": {"$ref": "#/$defs/callable_ref"},
            },
            "additionalProperties": False,

            "oneOf": [
                {"properties": {"format": {"const": "json"}}, "required": ["format", "json"]},
                {"properties": {"format": {"const": "text"}}, "required": ["format", "text"]},
                {"properties": {"format": {"const": "binary"}}, "required": ["format", "encoder"]},
            ],
        },

        # ----------------------------
        # Backend
        # ----------------------------
        "backend": {
            "type": "object",
            "required": ["type"],
            "properties": {
                "type": {"type": "string", "enum": ["websocket", "socketio", "zmq", "mqtt"]},
                # passed on as kwargs to respective Blueshift `DataSource`
                "options": {"type": "object", "additionalProperties": True},
            },
            "additionalProperties": False,
        },

        # ----------------------------
        # Parser & Router
        # ----------------------------
        "parser": {
            "type": "object",
            "required": ["format"],
            "properties": {
                "format": {"type": "string", "enum": ["json", "text", "binary"]},

                # Optional: transform raw payload before parsing (e.g. decompress)
                "preprocess": {"$ref": "#/$defs/callable_ref"},

                # For json: parse bytes->dict if needed; for text: decode; for binary: custom decode
                "decode": {"$ref": "#/$defs/callable_ref"},

                # For json: optional path where actual payload lives (some streams wrap it)
                "payload_path": {"$ref": "#/$defs/expr"},
            },
            "additionalProperties": False,
        },

        "router_rule": {
            "type": "object",
            "required": ["channel", "match"],
            "properties": {
                "channel": {"$ref": "#/$defs/channel"},
                "match": {"$ref": "#/$defs/expr"},
            },
            "additionalProperties": False,
        },

        "router": {
            "type": "object",
            "properties": {
                "enabled": {"type": "boolean", "default": True},

                # If multiple channels are present, rules decide classification.
                # First matching rule wins.
                "rules": {
                    "type": "array",
                    "minItems": 1,
                    "items": {"$ref": "#/$defs/router_rule"},
                },

                # Default classification if none matched
                "default_channel": {"$ref": "#/$defs/channel"},
            },
            "additionalProperties": False,
        },

        # ----------------------------
        # Converters: reference existing or inline partial converters
        # ----------------------------
        "converters": {
            "type": "object",
            "properties": {
                # per-channel conversion config
                "data": {"$ref": "#/$defs/streaming_data_conversion_pipeline"},
                "quote": {"$ref": "#/$defs/streaming_quote_conversion_pipeline"},
                "order": {"$ref": "#/$defs/conversion_pipeline"},
                "position": {"$ref": "#/$defs/conversion_pipeline"},
                "news": {"$ref": "#/$defs/conversion_pipeline"},
                "trade": {"$ref": "#/$defs/conversion_pipeline"},
            },
            "additionalProperties": False,
        },

        # ----------------------------
        # Subscribe / unsubscribe
        # ----------------------------
        "subscription": {
            "type": "object",
            "required": ["subscribe"],
            "properties": {
                #"subscribe": {"$ref": "#/$defs/message_def"},
                #"unsubscribe": {"$ref": "#/$defs/message_def"},
                # subscribed list keyed by channel of the subscriptions or "all"
                "subscribe": {
                    "type": "object",
                    "additionalProperties": {"$ref": "#/$defs/message_def"}
                    },
                # subscribed list keyed by channel of the subscriptions or "all"
                "unsubscribe": {
                    "type": "object",
                    "additionalProperties": {"$ref": "#/$defs/message_def"}
                    },
            },
            "additionalProperties": False,
        },

        # ----------------------------
        # Full connection definition
        # ----------------------------
        "stream_connection": {
            "type": "object",
            "required": ["url", "backend", "streams", "subscribe"],
            "properties": {
                "url": {"type": "string", "description": "Connection URL (ws://, wss://, tcp:// etc.)"},
                "backend": {"$ref": "#/$defs/backend"},
                "streams": {"$ref": "#/$defs/streams"},
                # if each update comes as a separate record, or can be a list of records
                "frame": {"type": "string", "enum": ["record", "array"], "default":"record"},
                # optional auth
                "auth": {"$ref": "#/$defs/auth"},

                # subscribe/unsubscribe commands
                "subscribe": {"$ref": "#/$defs/subscription"},

                # parsing incoming messages
                "parser": {"$ref": "#/$defs/parser"},

                # message routing (needed if multiple streams share one socket)
                "router": {"$ref": "#/$defs/router"},

                # interpreting parsed message into objects (optionally per channel)
                "converters": {"$ref": "#/$defs/converters"},

                # optional hooks for lifecycle
                "hooks": {
                    "type": "object",
                    "properties": {
                        "on_connect": {"$ref": "#/$defs/callable_ref"},
                        "on_disconnect": {"$ref": "#/$defs/callable_ref"},
                        "on_error": {"$ref": "#/$defs/callable_ref"},
                        "on_message": {"$ref": "#/$defs/callable_ref"},
                    },
                    "additionalProperties": False,
                },
            },
            "additionalProperties": False,

            "allOf": [
                # If streams has more than one (or includes multiple channels), recommend router rules.
                # (Schema can't easily express count-based logic without custom vocab; keep optional.)
            ],
        },
    },
}


def _extend_with_default(validator_class) -> Type[Validator]:
    validate_properties = validator_class.VALIDATORS["properties"]

    def set_defaults(validator, properties, instance, schema):
        for prop, subschema in properties.items():
                if isinstance(subschema, dict) and "default" in subschema and prop not in instance:
                    instance[prop] = deepcopy(subschema["default"])

        for error in validate_properties(validator, properties, instance, schema):
            yield error

    return extend(validator_class, {"properties": set_defaults})

def iter_errors_sorted(validator: Validator, instance: dict[str, Any]) -> list[SchemaValidationError]:
    """ sorted validation errors list """
    errors = list(validator.iter_errors(instance))
    # sort by depth (path length) and then by path name
    errors.sort(key=lambda e: (len(e.path), str(e.path)))
    return errors

def _format_friendly_error(error: SchemaValidationError) -> str:
    """ generate a human friendly error message from the validation error metadata """
    path_parts = []
    for p in error.path:
        if isinstance(p, int):
            path_parts.append(f"[{p}]")
        else:
            sep = "." if path_parts and not path_parts[-1].endswith("]") else ""
            path_parts.append(f"{sep}{p}")
    
    location = "".join(path_parts) or "(root)"
    reason = error.message
    
    if error.validator == 'required':
        # error.message format: "'field' is a required property"
        import re
        match = re.search(r"'(.*)' is a required property", error.message)
        if match:
            reason = f"Missing required field: '{match.group(1)}'"
            
    elif error.validator == 'additionalProperties':
        # error.message format: "Additional properties are not allowed ('foo' was unexpected)"
        import re
        match = re.search(r"\('(.*)' was unexpected\)", error.message)
        if match:
            reason = f"Unknown field provided: '{match.group(1)}'"
            
    elif error.validator == 'enum':
        allowed = [str(v) for v in error.validator_value] # type: ignore
        if len(allowed) > 10:
            allowed = allowed[:10] + ["..."]
        allowed_str = ", ".join(allowed)
        reason = f"Invalid value. Must be one of: [{allowed_str}]"
        
    elif error.validator == 'type':
        types = error.validator_value
        if isinstance(types, list):
            types = " or ".join(types)
        reason = f"Invalid type. Expected: {types}"
        
    elif error.validator == 'minItems':
        reason = f"List too short. Minimum items required: {error.validator_value}"
        
    elif error.validator == 'maxItems':
        reason = f"List too long. Maximum items allowed: {error.validator_value}"
        
    return f"Error @ {location}: {reason}"

def sanitize_config(config: Any, registry:ConfigRegistry) -> Any:
    """ recursively convert Enum keys/values and Callables to strings """
    if isinstance(config, dict):
        return {
            (k.name if isinstance(k, Enum) else k): sanitize_config(v, registry)
            for k, v in config.items()
        }
    elif isinstance(config, list):
        return [sanitize_config(i, registry) for i in config]
    elif isinstance(config, Enum):
        return config.name
    elif callable(config):
        name = getattr(config, "__name__", str(config))
        registry.register(name)(config) # discard the decorated, we are interested in the side-effect only
        return name
    else:
        return config

def validate_schema(schema: dict[str, Any], config: dict[str, Any], registry:ConfigRegistry) -> tuple[bool, list[str]]:
    """ validate the schema and return status and list of error messages. """
    formatted: list[str] = []
    validator = DefaultingValidator(schema)
    
    # sanitize config to handle Enum keys/values and Callables before validation
    config = sanitize_config(config, registry)
    
    errors = iter_errors_sorted(validator, config)

    if not errors:
        return True, formatted

    for e in errors:
        formatted.append(_format_friendly_error(e))
    
    return False, formatted

def apply_defaults(schema: dict[str, Any], instance: dict[str, Any], registry:ConfigRegistry) -> dict[str, Any]:
    # sanitize config to handle Enum keys/values and Callables before deepcopy/validation
    try:
        instance = sanitize_config(instance, registry)
        instance = deepcopy(instance)
        validator = DefaultingValidator(schema)
        
        errors = iter_errors_sorted(validator, instance)
        if errors:
            msgs = [_format_friendly_error(e) for e in errors]
            # raise one consolidated error
            raise ValidationError("\n".join(msgs))

        return instance
    except ValidationError:
        raise
    except Exception as e:
        raise ValidationError(str(e))

def get_global_api_spec(instance):
    keys = set(API_SCHEMA.get("properties", {}).keys())
    return {k:v for k,v in instance.items() if k in keys}

DefaultingValidator:Type[Validator] = _extend_with_default(Draft202012Validator)

__all__ = [
    'BROKER_SCHEMA',
    'API_SCHEMA',
    'STREAMING_SCHEMA',
    'validate_schema',
    'iter_errors_sorted',
    'apply_defaults',
    'get_global_api_spec',
    ]