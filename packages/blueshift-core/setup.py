import os
import sys
import numpy
from setuptools import setup, Extension, find_namespace_packages, Command
from setuptools.command.build_ext import build_ext
from Cython.Build import cythonize

# Directory containing the source code
SRC_DIR = 'src'
extensions = []

def find_extensions(search_path):
    """ find the cython files to build. """
    extensions = []
    for root, dirs, files in os.walk(search_path):
        for file in files:
            if file.endswith('.pyx'):
                file_path = os.path.join(root, file)
                rel_path = os.path.relpath(file_path, search_path)
                module_name = os.path.splitext(rel_path)[0].replace(os.sep, '.')
                ext = Extension(
                    module_name,
                    sources=[file_path],
                    include_dirs=[numpy.get_include(), search_path],
                    define_macros=[("NPY_NO_DEPRECATED_API", "NPY_1_7_API_VERSION")],
                )
                extensions.append(ext)
    return extensions

class CleanupCommand(Command):
    """custom command to clean up generated C/C++ files."""
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        print("\n[CleanupCommand] Cleaning up generated C/C++ files...")
        for root, dirs, files in os.walk(SRC_DIR):
            for file in files:
                if file.endswith(('.c', '.cpp', '.cxx')):
                    file_path = os.path.join(root, file)
                    # Check if a corresponding .pyx file exists to be on the safer side
                    base_name = os.path.splitext(file)[0]
                    pyx_file = base_name + '.pyx'
                    if os.path.exists(os.path.join(root, pyx_file)):
                        abs_path = os.path.abspath(file_path)
                        print(f"Removing generated file: {abs_path}")
                        try:
                            os.remove(file_path)
                        except OSError as e:
                            print(f"Error removing {file_path}: {e}")

class BuildExt(build_ext):
    """ clean up the generated c/cpp sources. """
    global extensions
    extensions.extend(find_extensions(SRC_DIR))

    def run(self):
        super().run()

setup(
    name='blueshift-core',
    use_scm_version=True,
    package_dir={'': SRC_DIR},
    packages=find_namespace_packages(where=SRC_DIR),
    ext_modules=cythonize(
        extensions,
        compiler_directives={'language_level': "3"},
        annotate=False,
        include_path=[SRC_DIR]
    ),
    cmdclass={
        'build_ext': BuildExt,
        'cleanup': CleanupCommand,
    },
    zip_safe=False,
)
