"""Generate local development secrets without any third-party dependency."""
import base64
from pathlib import Path
import secrets

ROOT = Path(__file__).resolve().parents[1]


def main():
    target = ROOT / ".env"
    if target.exists():
        print(".env already exists; unchanged. Do not rotate keys on an existing database casually.")
        return
    lines = []
    for line in (ROOT / ".env.example").read_text(encoding="utf-8").splitlines():
        if "=REPLACE_ME" in line:
            name = line.split("=", 1)[0]
            value = (base64.urlsafe_b64encode(secrets.token_bytes(32)).decode()
                     if name == "AIRFLOW_FERNET_KEY" else secrets.token_hex(24))
            line = f"{name}={value}"
        lines.append(line)
    with target.open("x", encoding="utf-8", newline="\n") as output:
        output.write("\n".join(lines) + "\n")
    target.chmod(0o600)
    print("Created .env. Open it locally to see passwords. Keep this file private.")


if __name__ == "__main__":
    main()
