@echo off
echo Compilando arquivos Java...
javac -d bin src\*.java

if %errorlevel% neq 0 (
    echo Erro durante a compilação. Abortando...
    pause
    exit /b
)

echo Executando o Launcher...
java -cp bin Launcher 1 2

pause
