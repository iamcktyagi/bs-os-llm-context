from blueshift.interfaces.data.ingestor import set_builtin_ingestor_loader

def install_ingestor(ingestor_type=None):
    if ingestor_type is None or ingestor_type=='continuous' or ingestor_type=='streaming':
        from .stream_ingestor import StreamingData


set_builtin_ingestor_loader(install_ingestor)