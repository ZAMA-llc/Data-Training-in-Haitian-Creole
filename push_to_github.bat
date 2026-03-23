@echo off
title ZAMA - Push GitHub

echo.
echo =========================================
echo  ZAMA - Push sur GitHub
echo  github.com/ZAMA-llc/Data-Training-in-
echo  Haitian-Creole
echo =========================================
echo.

:: Verifye Git
git --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERREUR] Git pas installe!
    echo Telecharger: https://git-scm.com/download/win
    pause
    exit /b 1
)

:: Aller dans le dossier du script
cd /d "%~dp0"
echo [INFO] Dossier: %cd%
echo.

:: =========================================
:: DECONNEXION KONT AKTYEL LA
:: =========================================
echo [INFO] Deconnexion compte GitHub actuel...

:: Efase credential Windows
cmdkey /delete:LegacyGeneric:target=git:https://github.com >nul 2>&1
cmdkey /delete:LegacyGeneric:target=git:https://github.com/ >nul 2>&1

:: Efase via Git credential manager
git credential reject >nul 2>&1

:: Efase config user aktyel
git config --global --unset user.name >nul 2>&1
git config --global --unset user.email >nul 2>&1
git config --global --unset credential.helper >nul 2>&1

echo [OK] Compte precedent deconnecte.
echo.

:: =========================================
:: KONEKTE AK KONT ZAMA-LLC
:: =========================================
echo [INFO] Configuration compte ZAMA-llc...
git config --global user.name "ZAMA-llc"
git config --global user.email "administration@zamasof.com"
git config --global credential.helper manager
echo [OK] Compte ZAMA-llc configure.
echo.

:: =========================================
:: INIT REPO SI PA EGZISTE
:: =========================================
if not exist ".git" (
    echo [INFO] Initialisation repo Git...
    git init
    git remote add origin https://github.com/ZAMA-llc/Data-Training-in-Haitian-Creole.git
    echo [OK] Repo initialise.
) else (
    echo [INFO] Repo Git existe deja.
    git remote set-url origin https://github.com/ZAMA-llc/Data-Training-in-Haitian-Creole.git
    echo [OK] Remote mis a jour.
)
echo.

:: =========================================
:: ADD, COMMIT, PUSH
:: =========================================
echo [INFO] Branch main...
git branch -M main 2>nul

echo [INFO] Ajout fichiers...
git add .

echo.
git diff --staged --quiet
if %errorlevel% equ 0 (
    echo [INFO] Aucun changement a pousser.
    pause
    exit /b 0
)

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
echo.
echo [IMPORTANT] Git va demander vos identifiants:
echo   Username: ZAMA-llc
echo   Password: Votre Personal Access Token
echo             (PAS votre mot de passe GitHub)
echo.
echo Pour creer un token si vous n'en avez pas:
echo   1. Connectez-vous sur github.com avec ZAMA-llc
echo   2. Settings - Developer Settings
echo   3. Personal access tokens - Tokens (classic)
echo   4. Generate new token - Cocher "repo"
echo   5. Copier le token et coller ici
echo.

git push -u origin main
if %errorlevel% neq 0 (
    echo.
    echo [ERREUR] Push echoue.
    echo.
    echo Essayez avec le token directement:
    echo   git remote set-url origin https://ZAMA-llc:VOTRE_TOKEN@github.com/ZAMA-llc/Data-Training-in-Haitian-Creole.git
    echo   git push -u origin main
    echo.
    pause
    exit /b 1
)

echo.
echo =========================================
echo  SUCCES! Code pousse sur GitHub!
echo.
echo  Aller sur:
echo  github.com/ZAMA-llc/
echo  Data-Training-in-Haitian-Creole
echo.
echo  Prochain etap:
echo  Actions - Run workflow - ultimate
echo =========================================
echo.
pause
