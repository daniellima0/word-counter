@echo off
setlocal enabledelayedexpansion

:: ===== CONFIGURATION =====
set REDUCER_COUNT=2
set INPUT_FOLDER=input
set OUTPUT_FOLDER=output
set SRC_FOLDER=src
:: =========================

echo Compiling Java source files in %SRC_FOLDER%...

:: Compile all Java files inside src and output .class to same folder
javac %SRC_FOLDER%\*.java

if %errorlevel% neq 0 (
    echo Compilation failed. Please check your Java files.
    pause
    exit /b
)

:: Count the number of .txt files in the input folder
set MAP_COUNT=0
for %%F in (%INPUT_FOLDER%\*.txt) do (
    set /a MAP_COUNT+=1
)

if %MAP_COUNT%==0 (
    echo No .txt files found in %INPUT_FOLDER%.
    pause
    exit /b
)

echo Starting Coordinator with %MAP_COUNT% Mappers and %REDUCER_COUNT% Reducers...
start cmd /k java -cp %SRC_FOLDER% Coordinator %REDUCER_COUNT%

:: Wait a few seconds for the Coordinator to open its sockets
timeout /t 2 > nul

:: Start Mappers (1 per input file)
set i=0
for %%F in (%INPUT_FOLDER%\*.txt) do (
    echo Launching Mapper !i!
    start cmd /c java -cp %SRC_FOLDER% Mapper
    set /a i+=1
)

:: Start Reducers
for /L %%R in (1,1,%REDUCER_COUNT%) do (
    echo Launching Reducer %%R
    start cmd /k java -cp %SRC_FOLDER% Reducer
)

echo All processes launched.
pause
