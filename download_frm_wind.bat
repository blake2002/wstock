@echo off

set begin_date=20201021
set end_date=20201024
set python_path=D:\ProgramData\Anaconda3
set py_script_path=D:\demo\stock_analyz

echo stock

cd %py_script_path%
start %python_path%\python.exe %py_script_path%\stock.py D %begin_date% %end_date% 0
TIMEOUT /T 20
start %python_path%\python.exe %py_script_path%\stock.py D %begin_date% %end_date% 1
TIMEOUT /T 20
start %python_path%\python.exe %py_script_path%\stock.py D %begin_date% %end_date% 2
TIMEOUT /T 20
start %python_path%\python.exe %py_script_path%\stock.py D %begin_date% %end_date% 3

pause