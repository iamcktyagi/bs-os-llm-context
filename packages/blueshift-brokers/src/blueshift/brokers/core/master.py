from __future__ import annotations
from typing import TYPE_CHECKING, Any, cast
import os
import io
import json
import fnmatch
import pandas as pd
import zipfile
import tarfile
from datetime import datetime
from pathlib import Path

from blueshift.config import blueshift_cache_dir
from blueshift.providers.data.source.csvsource import SourceWalker
from blueshift.interfaces.assets.assets import asset_factory
from blueshift.interfaces.assets._assets import MarketData, Option
from blueshift.interfaces.assets.opt_chains import OptionChains
from blueshift.lib.exceptions import InitializationError, ServerError, ValidationError

if TYPE_CHECKING:
    from blueshift.brokers.core.broker import RestAPIBroker
    from .config.instruments import AssetDef
    from .config.api import MasterDataConfig

_FORMAT_STR = "brkr_master_{}[{}]_"
_CHAR_LIMIT = 5
_CHUNK_SIZE = 10000

def _clean_expired_files(broker: RestAPIBroker):
    pattern = _FORMAT_STR.format(broker.name[:_CHAR_LIMIT], broker.variant[:_CHAR_LIMIT])
    dir = Path(blueshift_cache_dir())
    files = list(dir.glob(pattern))
    dir_path = str(dir)
    today = pd.Timestamp.now(tz=broker.tz).normalize()

    for f in files:
        f_name = str(f)
        full = os.path.join(dir_path, f_name)
        
        try:
            dt = pd.Timestamp(f_name.split('_')[0])
            if dt < today:
                os.remove(full)
        except Exception:
            continue

def _infer_extension(master: MasterDataConfig):
    ext = master.format
    if master.compression and master.compression != 'none':
        if master.compression == 'gzip':
            ext += '.gz'
        else:
            ext += f'.{master.compression}'

    return ext

def _get_cache_path(broker: RestAPIBroker, master: MasterDataConfig, auto_remove:bool=True) -> str:
    """ Generate a cache path for the master data based on broker name and current date. """
    if auto_remove:
        _clean_expired_files(broker)

    cache_dir = Path(blueshift_cache_dir(create=True))
    dt = pd.Timestamp.now(tz=broker.tz).normalize()
    date_str = dt.strftime('%Y-%m-%d')
    
    ext = _infer_extension(master)
    suffix = master.endpoint.name
    formatted = _FORMAT_STR.format(broker.name[:_CHAR_LIMIT], broker.variant[:_CHAR_LIMIT])
    filename = f"{date_str}{formatted}{suffix}.{ext}"
    return os.path.join(cache_dir, filename)

def _fetch_from_api(broker: RestAPIBroker, master_config: MasterDataConfig) -> list[dict[str, Any]]:
    endpoint = master_config.endpoint
    data = broker.api.request(endpoint, logger=broker.logger, check_latency=False)

    if not isinstance(data, list):
        raise ValidationError(f'instrument fetch @{endpoint} - expected a list.')
    
    if data and not isinstance(data[0], dict):
        raise ValidationError(f'instrument fetch @{endpoint} - expected a list of dicts.')
    
    return data

def _download_to_disk(broker: RestAPIBroker, path: str, master_config: MasterDataConfig):
    endpoint = master_config.endpoint
    resp = broker.api.download(endpoint, logger=broker.logger)
    
    if master_config.compression == "none":
        # direct csv or json files
        with open(path, 'w') as fp:
            if master_config.format == 'json':
                data = pd.read_json(io.StringIO(resp.text))
                data.to_json(path)
            else:
                data = pd.read_csv(io.StringIO(resp.text), skipinitialspace=True)
                data.columns = data.columns.str.strip()
                data.to_csv(path)
    else:
        # archived compressed file
        with open(path, 'wb') as fp:
            for chunk in resp.iter_bytes():
                if chunk:
                    fp.write(chunk)


def _process_master_config(broker: RestAPIBroker, master_config: MasterDataConfig):
    if master_config.mode == 'list':
        data = _fetch_from_api(broker, master_config)
        for asset_def in master_config.assets:
            _process_data_chunk(broker, data, 'default', asset_def)
        return

    cache_path = _get_cache_path(broker, master_config)
    if not os.path.exists(cache_path):
        broker.logger.info(f"Downloading master data to {cache_path}...")
        _download_to_disk(broker, cache_path, master_config)
    else:
        broker.logger.info(f"Using cached master data from {cache_path}")

    ext = _infer_extension(master_config)
    walker = SourceWalker(cache_path, ext)
    
    for _, name, fp in walker:
        _process_file_stream(broker, fp, str(name), master_config)

def _process_file_stream(broker: RestAPIBroker, fp, filename, master_config: MasterDataConfig):
    fmt = master_config.format
    csv_options = master_config.csv_options.copy() or {}
    chunksize = csv_options.pop('chunksize', _CHUNK_SIZE)
    chunks = []

    if fmt in ('csv', 'txt'):
        try:
            chunks = pd.read_csv(fp, chunksize=chunksize, **csv_options)
        except Exception as e:
            broker.logger.error(f"Error reading CSV {filename}: {e}")
            return
    elif fmt == 'json':
        try:
            if csv_options.get('lines', False):
                chunks = pd.read_json(fp, lines=True, chunksize=chunksize, **csv_options)
            else:
                data = pd.read_json(fp, **csv_options)
                chunks = [data]
        except Exception as e:
            broker.logger.error(f"Error reading JSON {filename}: {e}")
            return
    
    for chunk in chunks:
        for asset_def in master_config.assets:
            if asset_def.file_match and asset_def.file_match not in str(filename):
                continue
            _process_data_chunk(broker, chunk, filename, asset_def)

def _process_data_chunk(broker:RestAPIBroker, data:pd.DataFrame|list[dict], filename:str, 
                        asset_def:AssetDef):
    ctx = broker.get_context()
        
    try:
        # Resolve pipeline (filtering and mapping)
        processed_items = asset_def.resolve(data, **ctx)
    except Exception as e:
        broker.logger.error(f"Failed to resolve assets for {filename}: {e}")
        return
        
    for item in processed_items:
        try:
            details = item.get('details', {})
            asset = asset_factory(**item)
            broker.asset_finder.update_asset_mapping(asset, details)
        except Exception as e:
            broker.logger.error(f'failed to create asset from master data for {filename}:{str(e)}')

def fetch_master_data(broker: RestAPIBroker):
    """ 
    Fetch master data from source (API/File), parse it, and return assets and option chains.
    Handles caching to disk to avoid repeated downloads in the same trading day.
    """ 
    master_configs = broker.config.master_data
    
    for config in master_configs:
        _process_master_config(broker, config)