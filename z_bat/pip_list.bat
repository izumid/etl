@ECHO off
TITLE [PIP: list]

set "path_env=%~dp0..\.venv\Scripts"
echo Enviroment path: %path_env%

call "%path_env%\activate.bat"
CALL "%path_env%\python" -m pip list
call "%path_env%\deactivate.bat"
TIMEOUT 10
EXIT