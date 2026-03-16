import pandas as pd
import re
import sys
import os
import dns.resolver
import json
import math
from datetime import datetime  # Importa a biblioteca datetime

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

# Função para validar a sintaxe do domínio


def validate_domain_syntax(domain):
    domain_regex = r'^(?!-)[A-Za-z0-9-]{1,63}(?<!-)(\.[A-Za-z]{2,})+$'
    return re.match(domain_regex, domain) is not None

# Função para carregar provedores confiáveis de um arquivo JSON


def load_trusted_providers(filename):
    try:
        with open(filename, 'r') as file:
            return json.load(file)
    except Exception as e:
        print(f"Erro ao carregar provedores confiáveis: {e}")
        return []

# Função para verificar se o domínio é de um provedor confiável


def is_trusted_provider(domain, trusted_providers):
    return domain.lower() in trusted_providers

# Função para verificar se o domínio tem um registro DNS válido


def has_valid_dns_record(domain):
    try:
        dns.resolver.resolve(domain, 'A')
        return True
    except Exception:
        return False

# Função para verificar se o domínio tem um registro MX válido


def has_valid_mx_record(domain):
    try:
        dns.resolver.resolve(domain, 'MX')
        return True
    except Exception:
        return False

# Função para validar o domínio com cache


def validate_domain(domain, trusted_providers, cacheDNS):
    domain = domain.strip().lower()

    # Verifica se o domínio está no cache
    if domain in cacheDNS:
        return cacheDNS[domain]

    validation_result = {
        'domain_syntax': validate_domain_syntax(domain),
        'trustedProvider': False,
        'dns': False,
        'mx': False
    }

    if validation_result['domain_syntax']:
        validation_result['trustedProvider'] = is_trusted_provider(
            domain, trusted_providers)

        if validation_result['trustedProvider']:
            validation_result['dns'] = True
            validation_result['mx'] = True
        else:
            validation_result['dns'] = has_valid_dns_record(domain)
            if validation_result['dns']:
                validation_result['mx'] = has_valid_mx_record(domain)

    # Armazena o resultado no cache
    cacheDNS[domain] = validation_result
    return validation_result


def process_email_file(input_file, output_file, trusted_providers):
    # Termos indesejados
    unwanted_terms = [
        'contato',
        'administra',
        'juridico',
        'admin@',
        'contab',
        'falecom',
        'financeiro',
        'webmaster',
        'admin',
        'support',
        'info',
        'contact',
        'sales',
        'billing',
        'abuse',
        'security',
        'postmaster',
        'responder',
        'noreply',
        'no-reply'
    ]

    # Ler o arquivo CSV
    try:
        # Primeira tentativa com utf-8
        df = pd.read_csv(input_file, delimiter=';', encoding='utf-8')
    except UnicodeDecodeError:
        print("Erro ao ler o arquivo com codificação UTF-8, tentando com ISO-8859-1...")
        # Segunda tentativa com ISO-8859-1
        df = pd.read_csv(input_file, delimiter=';', encoding='ISO-8859-1')
    except Exception as e:
        print(f"Erro ao ler o arquivo: {e}")
        return

    # Verificar se a coluna "email" existe
    if 'email' not in df.columns:
        print("A coluna 'email' não foi encontrada no arquivo.")
        return

    # Total de registros
    total_records = len(df)
    print(f"Total de registros a serem processados: {total_records}")

    # Cache de DNS
    cacheDNS = {}

    # Acumula lotes filtrados para escrita final (evita reabrir/ler o arquivo várias vezes)
    filtered_batches = []

    # Lote de processamento
    batch_size = max(1, total_records // 50)  # Divide em no máximo 50 lotes
    num_batches = math.ceil(total_records / batch_size)

    # Filtrar os emails válidos, sem termos indesejados e que têm domínio válido
    def filter_emails(email):
        # Ignora valores ausentes ou não-textuais
        if not isinstance(email, str):
            return False

        # Remove espaços em branco no início/fim
        email = email.strip()
        if email == "":
            return False

        # Verificar se a string termina com . e remover o ponto
        if email.endswith('.'):
            email = email[:-1]

        # Transformar o email para caixa baixa
        email = email.lower()

        if is_valid_email(email) and not contains_unwanted_terms(email, unwanted_terms):
            domain = email.split('@')[-1]
            domain_validation = validate_domain(
                domain, trusted_providers, cacheDNS)
            return domain_validation['domain_syntax'] and (domain_validation['trustedProvider'] or domain_validation['mx'] or domain_validation['dns'])
        return False

    # Processamento em lotes
    for batch_num in range(num_batches):
        start_index = batch_num * batch_size
        end_index = min(start_index + batch_size, total_records)

        print(
            f"Processando lote {batch_num + 1}/{num_batches} (registros {start_index + 1} a {end_index})...")

        # Filtra emails para o lote atual
        start_time = datetime.now()  # Registra a hora de início
        print(f"Início: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        batch_filtered = df.iloc[start_index:end_index]['email'].apply(
            filter_emails)
        filtered_batch = df.iloc[start_index:end_index][batch_filtered]

        # Tratamento do campo email
        if 'email' in filtered_batch.columns:
            filtered_batch['email'] = (
                filtered_batch['email']
                .str.lower()  # Converte para minúsculas
                .str.strip()  # Remove espaços em branco
                .str.replace(r'\.$', '', regex=True)  # Remove o ponto no final
            )

        # Acumula os resultados para gravar no final
        if not filtered_batch.empty:
            filtered_batches.append(filtered_batch)

        end_time = datetime.now()  # Registra a hora de término
        print(f"Término: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")

    # Grava o arquivo de saída (uma vez, após processar todos os lotes)
    if filtered_batches:
        result_df = pd.concat(filtered_batches, ignore_index=True)
        result_df.to_excel(output_file, index=False)

    # Exibir mensagem final
    print(f"Processamento concluído! Arquivo final salvo em: {output_file}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Uso: python script.py <arquivo_entrada.csv> <arquivo_saida.csv>")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]

    # Verificar se o arquivo de entrada existe
    if not os.path.exists(input_file):
        print(f"Arquivo de entrada {input_file} não encontrado.")
        sys.exit(1)

    # Carregar provedores confiáveis
    trusted_providers = load_trusted_providers('./trusted-providers.json')

    process_email_file(input_file, output_file, trusted_providers)
