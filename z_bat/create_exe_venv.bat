@ECHO off
TITLE [EXE: Based On Venv]

set "path_env=%~dp0..\.venv\Scripts"
set output_folder=%~dp0..\exe_file

echo Enviroment path: %path_env%

CALL "%path_env%\activate"
CALL "%path_env%\python" -m PyInstaller  ^
    --onefile  ^
    --icon="%~dp0..\img\icon.ico" ^
    --name "etl" ^
    --workpath "%output_folder%" ^
    --distpath "%output_folder%" ^
    --specpath "%output_folder%" ^
    "%~dp0..\main.py"

CALL "%path_env%\deactivate"
TIMEOUT 10
EXIT