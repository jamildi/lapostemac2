# La Poste Bot Mac

Ce depot sert a construire une vraie application macOS `La Poste Bot.app` via GitHub Actions.

## Objectif

- double-clic sur une app Mac
- pas de Terminal
- pas de Python a installer sur le Mac utilisateur
- Chromium embarque sous forme d'archive puis extrait au premier lancement

## Validation GitHub

Le workflow macOS :

- compile `app_laposte.py`
- construit `La Poste Bot.app` avec PyInstaller
- lance l'app compilee en `--smoke-test`
- lance l'app compilee en `--browser-smoke-test` pour verifier Playwright + Chromium
- publie l'artefact zippe `La Poste Bot Mac.zip`

## Artefact attendu

Apres un run GitHub Actions reussi, telecharger l'artefact :

- `la-poste-bot-mac-app`

Puis dezipper `La Poste Bot Mac.zip` sur le Mac et ouvrir `La Poste Bot.app`.
