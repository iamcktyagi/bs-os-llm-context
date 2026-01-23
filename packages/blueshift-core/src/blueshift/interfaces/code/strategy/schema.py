from __future__ import annotations
from typing import TYPE_CHECKING, Dict, Any, List, Optional
from enum import Enum
from jsonschema import validate, ValidationError as SchemaValidationError

from blueshift.lib.exceptions import ValidationError

if TYPE_CHECKING:
    import pandas as pd
else:
    import blueshift.lib.common.lazy_pandas as pd

_VERSION="4.0.0"

class StrategyType(Enum):
    PYTHON='python'
    BLOCKLY='visual'
    WIZARD='wizard'
    EXECUTION='execution'
    
PARAMS_SCHEMA = {
      "type": "object",
      "description": "Strategy-specific parameters",
      "properties": {
        "type": {
                "type": "string",
                "enum": ["text", "number", "time","date","trigger","dropdown"],
                "description": "Parameter type"
                },
        "is_mandatory":{
                "type":"bool",
                "description":"if a mandatory parameter"
                },
        "description": {
                "type":"string",
                "description":"parameter description"
                },
        "value":{
                "additionalProperties": True
                }
      },
      "required": ["type","value"]
    }

RISK_RATING = {
      "type": "string",
      "enum": ["low", "medium", "high"],
      "description": "Risk assessment rating for the strategy"
    }

ASSET_CLASS = {
      "type": "array",
      "description": "List of asset classes this strategy applies to",
      "items": {
        "type": "string"
      },
      "uniqueItems": True
    }
      
STRATEGY_TAGS = {
      "type": "array",
      "description": "Categorization tags for the strategy",
      "items": {
        "type": "string"
      },
      "uniqueItems": True
    }
      
PERFORMANCE = {
      "type": "object",
      "description": "Historical performance metrics",
      "properties": {
        "sharpe": {
          "type": "number",
          "description": "Sharpe ratio of the strategy"
        },
        "returns": {
          "type": "number",
          "description": "Historical returns percentage"
        },
        "drawdown": {
          "type": "number",
          "description": "Historical max drawdown percentage"
        },
        "start": {
          "type": "string",
          "format": "date",
          "description": "Start date of performance period (YYYY-MM-DD)"
        },
        "end": {
          "type": "string",
          "format": "date",
          "description": "End date of performance period (YYYY-MM-DD)"
        }
      },
      "required": ["sharpe", "returns", "drawdown", "start", "end"],
      "additionalProperties": False
    }

CAPITAL_SCHEMA = {
    "type": "object", 
    "properties": {
        "type": {
                "type": "string",
                "enum": ["free", "estimate", "fixed", "multiplier","linked"],
                "description": "Required capital definition"
                },
        "value":{
              "type": "number",
              "description": "Capital amount for the fixed case"
            },
        "linked": {"type": "string", "description":"linked parameter name"},
        "multipliers": {
          "type": "object",
          "description": "Multiplier values for different capital specs",
          "properties": {
            "max_multiplier": {
              "type": "number",
              "description": "Max allowed multiplier for multiplier case"
            },
            "A1": {
              "type": "number",
              "description": "Minimum capital multiplier for linked spec"
            },
            "B1": {
              "type": "number",
              "description": "Minimum capital constant for linked spec"
            },
            "A2": {
              "type": "number",
              "description": "Recommended capital multiplier for linked spec"
            },
            "B2": {
              "type": "number",
              "description": "Recommended capital constant for linked spec"
            }
          }
    },
    "required": ["type"]
    }
}
    
STRATEGY_SCHEMA = {
  "$schema": "http://json-schema.org/draft-07/schema#",
  "$defs": {
        "parameter": PARAMS_SCHEMA,
        "capital": CAPITAL_SCHEMA,
        "risk_rating": RISK_RATING,
        "asset_class": ASSET_CLASS,
        "tags": STRATEGY_TAGS,
        "performance": PERFORMANCE,
    },
  "type": "object",
  "title": "Trading Strategy Schema",
  "description": "Schema for defining trading strategy objects with parameters, risk ratings, and performance metrics",
  "properties": {
    "version": {
      "type": "string",
      "description": "Version identifier for the strategy"
    },
    "name": {
      "type": "string",
      "description": "Internal name/ hash of the strategy - must be unique"
    },
    "display_name": {
      "type": "string",
      "description": "Human-readable display name for the strategy"
    },
    "description": {
      "type": "string",
      "description": "Brief description of the strategy (text)"
    },
    "details": {
      "type": "string",
      "description": "Detailed markdown documentation of the strategy",
      "contentMediaType": "text/markdown"
    },
    "position_type": {
      "type": "string",
      "enum": ["intraday", "positional"],
      "description": "Positional or intraday strategies"
    },
    "type": {
      "type": "string",
      "enum": ['python', 'visual', 'wizard', 'execution'],
      "description": "Type classification of the strategy"
    },
    "code": {
      "type": "string",
      "description": "Strategy python code representation or stringified json"
    },
    "parameters": {
      "type": "object",
      "additionalProperties": {"$ref": "#/$defs/parameter"},
    },
    "capital": {"$ref": "#/$defs/capital"},
    "risk_rating": {"$ref": "#/$defs/risk_rating"},
    "asset_class": {"$ref": "#/$defs/asset_class"},
    "tags": {"$ref": "#/$defs/tags"},
    "performance": {"$ref": "#/$defs/performance"}
  },
  "required": [
    "name",
    "type",
    "code"
  ],
  "additionalProperties": True
}


class StrategyValidator:
    """
    Validator class for trading strategy objects using JSON Schema
    """
    
    def __init__(self):
        """
        Initialize validator with schema
        """
        self.schema = STRATEGY_SCHEMA
        self.validator = None
    
    def validate(self, data: Dict[str, Any]) -> bool:
        """
        Validate data against schema
        
        Args:
            data: Dictionary to validate
            
        Returns:
            True if valid, raises ValidationError if invalid
        """
        try:
            validate(instance=data, schema=self.schema)
            return True
        except SchemaValidationError as e:
            raise ValidationError(f"Validation failed: {e.message}")
            
        # extra validation
        if data['capital']['type'] == 'fixed':
            try:
                value = float(data['capital']['value'])
                if value <= 0:raise ValueError()
            except Exception:
                msg = f'Capital value must be specified for fixed capital type'
                raise ValidationError(f"Validation failed: {msg}")
        elif data['capital']['type'] == 'multiplier':
            try:
                mult = float(data['capital']['multipliers']['max_multiplier'])
                if mult <= 0:raise ValueError()
            except Exception:
                msg = f'Max multiplier must be specified for multiplier capital type'
                raise ValidationError(f"Validation failed: {msg}")
        elif data['capital']['type'] == 'linked':
            try:
                param = data['capital']['linked']
                if param not in data['parameters']:raise ValueError()
                a1 = float(data['capital']['multipliers']['A1'])
                a2 = float(data['capital']['multipliers']['A2'])
                b1 = float(data['capital']['multipliers']['B1'])
                b2 = float(data['capital']['multipliers']['B2'])
                if a1==0 or a2==0 or b1==0 or b2==0:raise ValueError()
            except Exception:
                msg = f'Linked parameter and multipliers must be specified'
                raise ValidationError(f"Validation failed: {msg}")
                
        if 'performance' in data:
            try:
                pd.Timestamp(data['performance']['start'])
                pd.Timestamp(data['performance']['end'])
            except Exception:
                msg = f'Start and end date must be valid dates'
                raise ValidationError(f"Validation failed: {msg}")
    
    def validate_safe(self, data: Dict[str, Any]) -> tuple[bool, Optional[str]]:
        """
        Safe validation that returns success status and error message
        
        Args:
            data: Dictionary to validate
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        try:
            validate(instance=data, schema=self.schema)
            return True, None
        except SchemaValidationError as e:
            return False, str(e.message)
    
    def get_validation_errors(self, data: Dict[str, Any]) -> List[str]:
        """
        Get all validation errors for the data
        
        Args:
            data: Dictionary to validate
            
        Returns:
            List of error messages
        """
        if self.validator is None:
            from jsonschema import Draft7Validator
            self.validator = Draft7Validator(self.schema)
            
        errors = []
        for error in self.validator.iter_errors(data):
            errors.append(f"Path: {'.'.join(str(p) for p in error.path)} | Error: {error.message}")
        return errors


