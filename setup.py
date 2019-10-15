import os
import sys
from cx_Freeze import setup, Executable
import matplotlib

PYTHON_PATH = os.path.dirname(sys.executable)

os.environ['TCL_LIBRARY'] = os.path.join(PYTHON_PATH, 'tcl', 'tcl8.6')
os.environ['TK_LIBRARY'] = os.path.join(PYTHON_PATH, 'tcl', 'tk8.6')

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
			'includes': additional_mods,
			'packages': ['idna', 'numpy', 'pandas'],
			'include_files': [(matplotlib.get_data_path(), "mpl-data")]
		}
	},
    executables = executables
)
