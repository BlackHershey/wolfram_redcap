import os
import sys
from cx_Freeze import hooks, setup, Executable
import matplotlib
import gooey

from getpass import getuser

gooey_path = os.path.split(gooey.__file__)[0]
include_files = [
	(matplotlib.get_data_path(), "mpl-data"), 
	(os.path.join(gooey_path, 'images'), 'gooey/images'), 
	(os.path.join(gooey_path, 'languages'), 'gooey/languages')]

PYTHON_PATH = os.path.dirname(sys.executable)

# os.environ['TCL_LIBRARY'] = os.path.join(PYTHON_PATH, 'tcl', 'tcl8.6')
# os.environ['TK_LIBRARY'] = os.path.join(PYTHON_PATH, 'tcl', 'tk8.6')
# NOTE: Jon had to hard-code this to the system Python path, couldn't figure out how to find it in the venv.
os.environ['TCL_LIBRARY'] = r'C:\Users\{}\AppData\Local\Programs\Python\Python38\tcl\tcl8.6'.format(getuser())
os.environ['TK_LIBRARY'] = r'C:\Users\{}\AppData\Local\Programs\Python\Python38\tcl\tk8.6'.format(getuser())

additional_mods = ['numpy.core._methods', 'numpy.lib.format', 'matplotlib.backends.backend_tkagg']

base = "Win32GUI" if sys.platform == "win32" else "console"

executables = [
	Executable('format_wolfram_data.py', base=base),
	Executable('format_track_data.py', base=base),
	Executable('nih_toolbox_import.py', base=base),
	Executable('dot_dbs_import.py', base=base),
    Executable('cbcl_aseba_import.py', base=base),
	Executable('cbcl_format_scores.py', base=base),
	Executable('redcap2spss.py', base=base),
	Executable('extract_form_fields.py', base=base),
	Executable('tools/slope.py', base=base),
    Executable('tools/extract_and_combine_volbrain.py', base=base)
]

setup(
	name = 'redcap scripts',
    version = "1.0",
    description = 'redcap scripts',
    options = {
		'build_exe': {
            'build_exe': '../build',
			'include_msvcr': True,
			'includes': additional_mods,
			'packages': ['idna', 'numpy'],
			'excludes': ['matplotlib.tests', 'numpy.random_examples'],
			'include_files': include_files
		}
	},
    executables = executables
)
