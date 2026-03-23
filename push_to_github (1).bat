@echo off
title ZAMA - Push GitHub

echo.
echo =========================================
echo  ZAMA - Push sur GitHub
echo  github.com/ZAMA-llc/Data-Training-in-
echo  Haitian-Creole
echo =========================================
echo.

git --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERREUR] Git pas installe!
    echo Telecharger: https://git-scm.com/download/win
    pause
    exit /b 1
)

cd /d "%~dp0"
echo [INFO] Dossier: %cd%
echo.

if not exist ".git" (
    echo [INFO] Initialisation repo Git...
    git init
    git remote add origin https://github.com/ZAMA-llc/Data-Training-in-Haitian-Creole.git
    echo [OK] Repo initialise.
) else (
    echo [INFO] Repo Git existe deja.
    git remote get-url origin >nul 2>&1
    if %errorlevel% neq 0 (
        git remote add origin https://github.com/ZAMA-llc/Data-Training-in-Haitian-Creole.git
        echo [OK] Remote ajoute.
    )
)

echo.
echo [INFO] Config Git...
git config user.name "ZAMA-llc"
git config user.email "administration@zamasof.com"

echo.
echo [INFO] Branch main...
git branch -M main 2>nul

echo.
echo [INFO] Ajout fichiers...
git add .

echo.
git diff --staged --quiet
if %errorlevel% equ 0 (
    echo [INFO] Aucun changement a pousser.
    pause
    exit /b 0
)

echo.
echo [INFO] Commit...
git commit -m "ZAMA - Add pipeline scripts"
if %errorlevel% neq 0 (
    echo [ERREUR] Commit echoue!
    pause
    exit /b 1
)

echo.
echo [INFO] Pull avant push...
git pull --rebase origin main 2>nul

echo.
echo [INFO] Push vers GitHub...
git push -u origin main
if %errorlevel% neq 0 (
    echo.
    echo [ATTENTION] Push echoue.
    echo.
    echo Si Git demande un mot de passe, utiliser un Token:
    echo   github.com - Settings - Developer Settings
    echo   Personal access tokens - Generate new token
    echo   Cocher "repo" - Copier le token
    echo   Coller le token comme mot de passe
    echo.
    pause
    exit /b 1
)

echo.
echo =========================================
echo  SUCCES! Code pousse sur GitHub!
echo  Aller sur: github.com/ZAMA-llc/
echo  Data-Training-in-Haitian-Creole
echo.
echo  Prochain etap:
echo  Actions - Run workflow - ultimate
echo =========================================
echo.
pause
