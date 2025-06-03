@echo off

pushd ..\nvim-portable
call start-shell.bat
popd

call .\venv\Scripts\activate.bat
start "" goneovim.exe
exit /b