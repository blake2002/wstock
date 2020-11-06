@echo off

set data_path=data_20201024
set python_path=D:\ProgramData\Anaconda3
set py_script_path=D:\demo\stock_analyz

echo stock

cd %py_script_path%
start %python_path%\python.exe %py_script_path%\stock.py IS %data_path%


pause