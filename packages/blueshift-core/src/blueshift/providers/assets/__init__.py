from blueshift.interfaces.assets.assetdb import set_builtin_assetdb_loader

def assetdb_installer(assetdb_type=None):
    if assetdb_type is None or assetdb_type=='dict':
        from .dictdb import DictDBDriver

set_builtin_assetdb_loader(assetdb_installer)