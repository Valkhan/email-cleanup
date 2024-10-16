import pandas as pd
import re
import sys
import os

# Função para verificar se o email é válido
def is_valid_email(email):
    email_regex = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(email_regex, email) is not None

# Função para remover emails com termos indesejados
def contains_unwanted_terms(email, unwanted_terms):
    for term in unwanted_terms:
        if term.lower() in email.lower():
            return True
    return False

def process_email_file(input_file, output_file):
    # Termos indesejados
    unwanted_terms = ['contato', 'administra', 'juridico', 'admin@', 'contab', 'falecom', 'financeiro', 'webmaster']

    # Ler o arquivo Excel
    try:
        df = pd.read_csv(input_file, delimiter=';', skip_blank_lines=True)
    except Exception as e:
        print(f"Erro ao ler o arquivo: {e}")
        return

    # Verificar se a coluna "correio_eletronico" existe
    if 'correio_eletronico' not in df.columns:
        print("A coluna 'correio_eletronico' não foi encontrada no arquivo.")
        return

    # Filtrar os emails válidos e sem termos indesejados
    df_filtered = df[df['correio_eletronico'].apply(lambda email: is_valid_email(email) and not contains_unwanted_terms(email, unwanted_terms))]

    # Salvar o arquivo Excel filtrado
    try:
        df_filtered.to_excel(output_file, index=False)
        print(f"Arquivo processado com sucesso! Salvo em: {output_file}")
    except Exception as e:
        print(f"Erro ao salvar o arquivo: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Uso: python script.py <arquivo_entrada.xlsx> <arquivo_saida.xlsx>")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]

    # Verificar se o arquivo de entrada existe
    if not os.path.exists(input_file):
        print(f"Arquivo de entrada {input_file} não encontrado.")
        sys.exit(1)

    process_email_file(input_file, output_file)
