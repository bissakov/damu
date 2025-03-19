@echo off
cd %~dp0

.\venv\Scripts\python.exe -m timeit -s "from src import main" -n 1 "main.main()"