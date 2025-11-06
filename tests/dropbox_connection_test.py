import os
from dotenv import load_dotenv
import dropbox

# 1) Carrega o .env
load_dotenv()

token = os.environ.get("DROPBOX_TOKEN")

if not token:
    raise RuntimeError("DROPBOX_TOKEN não encontrado. Verifique o .env na raiz do projeto.")

# 2) Remover espaços/quebras de linha acidentais
token = token.strip()

print(f"Token carregado com sucesso. Tamanho: {len(token)} caracteres")

# 3) Cria o cliente Dropbox
dbx = dropbox.Dropbox(token)

# 4) Testa quem é o usuário
try:
    account = dbx.users_get_current_account()
    print("Conectado como:", account.name.display_name)
except dropbox.exceptions.AuthError as e:
    print("Falha de autenticação na API do Dropbox:")
    print(e)
    raise