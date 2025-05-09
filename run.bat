@echo off
echo Compiling Java files...
javac -d bin src\*.java

if %errorlevel% neq 0 (
    echo Compilation error. Aborting...
    pause
    exit /b
)

echo Running the Launcher...
java -cp bin Launcher 1 2

pause
