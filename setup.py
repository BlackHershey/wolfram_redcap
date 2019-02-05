import os
import sys
from cx_Freeze import setup, Executable
import matplotlib
# os.environ['TCL_LIBRARY'] = r'C:\Users\acevedoh\Downloads\WinPython-64bit-2.7.13.1Zero\python-2.7.13.amd64\tcl\tcl8.5'
# os.environ['TK_LIBRARY'] = r'C:\Users\acevedoh\Downloads\WinPython-64bit-2.7.13.1Zero\python-2.7.13.amd64\tcl\tk8.5'

os.environ['TCL_LIBRARY'] = r'C:\Users\acevedoh\Downloads\WinPython-64bit-3.5.4.0Qt5\python-3.5.4.amd64\tcl\tcl8.6'
os.environ['TK_LIBRARY'] = r'C:\Users\acevedoh\Downloads\WinPython-64bit-3.5.4.0Qt5\python-3.5.4.amd64\tcl\tk8.6'

additional_mods = ['numpy.core._methods', 'numpy.lib.format', 'matplotlib.backends.backend_tkagg']

if sys.platform == "win32":
    base = "Win32GUI"

executables = [
	Executable('format_wolfram_data.py', base=base),
	Executable('format_track_data.py', base=base),
	Executable('nih_toolbox_import.py', base=base),
	Executable('dot_dbs_import.py', base=base),
	Executable('cbcl_import.py', base=base),
	Executable('score_ycbcl.py', base=base),
	Executable('extract_cbcl_scores.py', base=base),
	Executable('redcap2spss.py', base=base),
	Executable('extract_form_fields.py', base=base),
	Executable('slope.py', base=base)
]
setup(  name = 'redcap scripts',
        version = "0.1",
        description = 'redcap scripts',
        options = {'build_exe': {
					'includes': additional_mods,
					'packages': ['idna'],
					'include_files': [(matplotlib.get_data_path(), "mpl-data")] \
						+ [ 'dependencies/' + dll for dll in ['tcl86t.dll', 'tk86t.dll'] ]
		}},
        executables = executables)
