@echo off
setlocal
cd /d "%~dp0\.."
call mvn -pl flowgate-simulation -am clean install
if errorlevel 1 exit /b 1
cd flowgate-simulation
call mvn exec:java -Dexec.args="..\benchmark-results\latest"
if errorlevel 1 exit /b 1
py -m pip install -r requirements.txt
if errorlevel 1 exit /b 1
py scripts\generate_report.py --input ..\benchmark-results\latest --output ..\benchmark-results\latest\report
if errorlevel 1 exit /b 1
echo.
echo Benchmark and report generation complete.
echo Open ..\benchmark-results\latest\report\report.html
endlocal
