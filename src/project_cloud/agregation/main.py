"""
Script principal pour executer toutes les agregations
"""
import sys
from datetime import datetime
import subprocess
import os

def main():
    # Obtenir le chemin du dossier d'agregation
    agregation_dir = os.path.dirname(os.path.abspath(__file__))

    # Liste des scripts d'agregation a executer
    scripts = [
        ("Parkings", "agregation_parkings.py"),
        ("Trafic Routier", "agregation_trafic.py"),
        ("SNCF Departures", "agregation_sncf.py")
    ]

    resultats = []

    for nom, script_name in scripts:
        print(f"EXECUTION: {nom}")

        script_path = os.path.join(agregation_dir, script_name)

        try:
            # Executer le script Python
            result = subprocess.run(
                [sys.executable, script_path],
                capture_output=True,
                text=True,
                timeout=600  # 10 minutes max par script
            )

            if result.returncode == 0:
                resultats.append((nom, "SUCCESS", None))
                print(result.stdout)
                print(f"\n[OK] {nom} - Agregation terminee avec succes")
            else:
                error_msg = result.stderr if result.stderr else "Erreur inconnue"
                resultats.append((nom, "FAILED", error_msg))
                print(result.stdout)
                print(f"\n[ERREUR] {nom}")
                print(error_msg)

        except subprocess.TimeoutExpired:
            error_msg = "Timeout: le script a depasse 10 minutes d'execution"
            resultats.append((nom, "FAILED", error_msg))
            print(f"\n[ERREUR] {nom} - {error_msg}")

        except Exception as e:
            error_msg = f"Erreur: {str(e)}"
            resultats.append((nom, "FAILED", error_msg))
            print(f"\n[ERREUR] {nom} - {error_msg}")
            print(f"Type d'erreur: {type(e).__name__}")

    # Affichage du resume
    print("RESUME DES AGREGATIONS")

    success_count = sum(1 for _, status, _ in resultats if status == "SUCCESS")
    failed_count = len(resultats) - success_count

    for nom, status, error in resultats:
        status_symbol = "OK" if status == "SUCCESS" else "KO"
        print(f"[{status_symbol}] {nom}: {status}")
        if error:
            # Afficher seulement les premieres lignes de l'erreur
            error_lines = error.strip().split('\n')
            for line in error_lines[:5]:
                print(f"     {line}")
            if len(error_lines) > 5:
                print(f"     ... ({len(error_lines) - 5} lignes supplementaires)")


    print(f"Total: {len(resultats)} agregations")
    print(f"Reussies: {success_count}")
    print(f"Echouees: {failed_count}")


    # Code de retour
    return 0 if failed_count == 0 else 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
