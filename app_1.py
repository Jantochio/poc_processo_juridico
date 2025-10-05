from datetime import date, timedelta
import re
import pandas as pd
import streamlit as st
from snowflake.snowpark.session import Session
from string import Template

# ---------- CONFIGURAÇÃO DO SNOWFLAKE ---------- #

def create_snowflake_session():
    return Session.builder.configs(st.secrets).create()

session = create_snowflake_session()

# ---------- FUNÇÕES AUXILIARES ---------- #

def calculate_deadline(days_text):
    """Calcula a data correspondente ao número de dias úteis."""
    match = re.search(r"(\d+)\s*dias úteis", days_text, re.IGNORECASE)
    if match:
        num_days = int(match.group(1))
        today = date.today()
        # Considerando dias corridos para simplificar
        deadline_date = today + timedelta(days=num_days)
        return deadline_date
    return None

def extract_total_value(petition_text):
    """Extrai o valor monetário total do texto."""
    petition_text = petition_text.replace(r"\$", "$")
    value_matches = re.findall(r"R\$ ?([\d.,]+)", petition_text)
    total = 0
    for value in value_matches:
        value_numeric = float(value.replace('.', '').replace(',', '.'))
        total += value_numeric
        if total > 0:
            break
    return total  # Valor numérico
    
def extract_information(petition_text):
    """Extrai informações principais da petição."""
    author_match = re.search(r"Autor[a]?:\s*([^\n\r]+)", petition_text, re.IGNORECASE)
    author = author_match.group(1).strip() if author_match else "Não encontrado"

    deadline_match = re.search(r"(\d+)\s*dias úteis", petition_text, re.IGNORECASE)
    if deadline_match:
        deadline = calculate_deadline(deadline_match.group(0))  # Calcula a data
    else:
        deadline = None

    case_value = extract_total_value(petition_text)

    if "extravio de bagagens" in petition_text.lower():
        case_type = "Extravio de Bagagens"
    # Verificar se "voo" e "atraso" estão na mesma linha (independente da ordem)
    elif any(("voo" in line and "atraso" in line) for line in petition_text.lower().splitlines()):
        case_type = "Indenização por atraso de voo"
    elif "danos morais" in petition_text.lower():
        case_type = "Indenização por Danos Morais"
    else:
        case_type = "Outros"

    description = ". ".join(petition_text.split("\n")[-3:]).strip()

    return {
        "Autor": author,
        "Prazo": str(deadline) if deadline else "Não encontrado",
        "Valor do Caso": case_value,
        "Tipo de Caso": case_type,
        "Descricao": description or "Não encontrado", # Corrigido: 'Descrição' para 'Descricao'
    }

def generate_contestation(case_type, extracted_data):
    """Gera a contestação com base no tipo de caso."""
    templates = {
        "Extravio de Bagagens": """

        Template:Extravio de Bagagens
        
        EXCELENTÍSSIMO(A) SENHOR(A) DOUTOR(A) JUIZ(A) DE DIREITO DA ___ VARA CÍVEL DA COMARCA DE ___.

        Processo nº: A SER DEFINIDO

        Réu: EMPRESA AÉREA S.A.  
        Autor: ${Autor}  

        ***CONTESTAÇÃO***

        1. **DOS FATOS**  
        ${Descricao}  

        2. **DA FUNDAMENTAÇÃO JURÍDICA**  
        Requer a improcedência da ação inicial com fundamento na legislação aplicável.  

        3. **DOS PEDIDOS**
        a) Improcedência da ação;  
        b) Custas processuais ao autor.  

        Termos em que pede deferimento.
        """,
        "Indenização por atraso de voo": """
        
        Template:Indenização por atraso de voo
        
        EXCELENTÍSSIMO(A) SENHOR(A) DOUTOR(A) JUIZ(A) DE DIREITO DA ___ VARA CÍVEL DA COMARCA DE ___.

        Processo nº: A SER DEFINIDO

        Réu: EMPRESA AÉREA S.A.  
        Autor: ${Autor}  

        ***CONTESTAÇÃO***

        1. **DOS FATOS**  
        ${Descricao}  

        2. **DA FUNDAMENTAÇÃO JURÍDICA**  
        Fundamentação baseada no argumento de ausência de responsabilidade pelo atraso.

        3. **DOS PEDIDOS**
        a) Improcedência da ação;  
        b) Custas processuais ao autor.  

        Termos em que pede deferimento.
        """,
        "Indenização por Danos Morais": """

        Template:Indenização por Danos Morais
        
        EXCELENTÍSSIMO(A) SENHOR(A) DOUTOR(A) JUIZ(A) DE DIREITO DA ___ VARA CÍVEL DA COMARCA DE ___.

        Processo nº: A SER DEFINIDO

        Réu: EMPRESA AÉREA S.A.  
        Autor: ${Autor}  

        ***CONTESTAÇÃO***

        1. **DOS FATOS**  
        ${Descricao}  

        2. **DA FUNDAMENTAÇÃO JURÍDICA**  
        Argumentação principal baseada na inexistência de danos morais.  

        3. **DOS PEDIDOS**
        a) Improcedência da ação;  
        b) Custas processuais ao autor.  

        Termos em que pede deferimento.
        """,
        "Outros": """

        Template:Outros
        
        EXCELENTÍSSIMO(A) SENHOR(A) DOUTOR(A) JUIZ(A) DE DIREITO DA ___ VARA CÍVEL DA COMARCA DE ___.

        Processo nº: A SER DEFINIDO

        Réu: EMPRESA AÉREA S.A.  
        Autor: ${Autor}  

        ***CONTESTAÇÃO***

        1. **DOS FATOS**  
        ${Descricao}  

        2. **DA FUNDAMENTAÇÃO JURÍDICA**  
        Fundamentação com base em análise do caso.  

        3. **DOS PEDIDOS**
        a) Improcedência da ação;  
        b) Custas processuais ao autor.  

        Termos em que pede deferimento.
        """
    }
    
    # Obtém o template correto
    template = Template(templates.get(case_type, templates["Outros"]))

    # Substituir os placeholders no template. Passamos as chaves como argumentos nomeados.
    try:
        filled_template = template.substitute(
            Autor=extracted_data["Autor"],
            Descricao=extracted_data["Descricao"] # Corrigido: 'Descrição' para 'Descricao'
        )
    except KeyError as e:
        # Informa qual chave está faltando de forma mais clara
        st.error(f"Erro: Placeholder ausente no dicionário para substituição - {e}. "
                 "Verifique se as chaves 'Autor' e 'Descricao' existem nos dados extraídos.")
        raise
    except Exception as e:
        st.error(f"Erro inesperado ao gerar a contestação: {e}")
        raise

    return filled_template

# ---------- INTERFACE STREAMLIT ---------- #

st.title("Processador Jurídico:")

# Inicializar variáveis no estado da sessão
if "extracted_data" not in st.session_state:
    st.session_state.extracted_data = None  # Dados extraídos
if "contestation" not in st.session_state:
    st.session_state.contestation = None  # Contestação gerada
if "processed" not in st.session_state:
    st.session_state.processed = False  # Flag para controlar exibição do botão "Salvar no Banco"

# Upload do arquivo da petição
uploaded_file = st.file_uploader("Faça o upload da petição inicial (.txt)", type=["txt"])

if uploaded_file:
    petition_text = uploaded_file.read().decode("utf-8")
    st.subheader("Texto da Petição")
    st.text_area("Conteúdo da Petição", petition_text, height=300)

    if st.button("Processar Petição"):
        st.session_state.extracted_data = extract_information(petition_text)
        st.session_state.contestation = generate_contestation(
            st.session_state.extracted_data["Tipo de Caso"], st.session_state.extracted_data
        )

        st.subheader("Informações Extraídas")
        st.json(st.session_state.extracted_data)

        st.subheader("Contestação Gerada:")
        st.text_area("Modelo", st.session_state.contestation, height=300)

        # Marcar que a petição foi processada
        st.session_state.processed = True

# Exibir o botão "Salvar no Banco de Dados" apenas após processar a petição
if st.session_state.processed:
    if st.button("Salvar no Banco de Dados"):
        if st.session_state.extracted_data and st.session_state.contestation:
            try:
                data_to_insert = pd.DataFrame([{
                    "RAW_TEXT": petition_text,
                    "AUTHOR_NAME": st.session_state.extracted_data["Autor"],
                    "DEADLINE": st.session_state.extracted_data["Prazo"],
                    "CASE_VALUE": st.session_state.extracted_data["Valor do Caso"],
                    "CASE_TYPE": st.session_state.extracted_data["Tipo de Caso"],
                    "CASE_DESCRIPTION": st.session_state.extracted_data["Descricao"], # Corrigido para 'Descricao'
                    "CONTESTATION_MODEL": st.session_state.contestation
                }])
                session.write_pandas(
                    df=data_to_insert,
                    table_name="PETITIONS",
                    database="LEGAL_CASE_MANAGEMENT",
                    schema="CONTESTATION_POC"
                )
                st.success("Petição salva no banco com sucesso!")
            except Exception as e:
                st.error(f"Erro ao salvar no banco: {str(e)}")
        else:
            st.warning("Processar a petição antes de salvar no banco!")

# ---------- CONSULTA AO BANCO DE DADOS ---------- #

#st.header("Petições Processadas")
try:
    petitions = session.sql(
        """
        SELECT --CASE_ID AS ID_CASO,
               AUTHOR_NAME AS AUTOR,
               DEADLINE AS PRAZO,
               CASE_VALUE AS VALOR,
               CASE_TYPE AS TIPO_CASO,
               CASE_DESCRIPTION AS DESCRICAO,
               UPLOADED_AT AS DATA_INCLUSAO,
               RAW_TEXT AS DADOS_PETICAO,
               CONTESTATION_MODEL AS MODELO_GERADO
        FROM LEGAL_CASE_MANAGEMENT.CONTESTATION_POC.PETITIONS
        """
    ).to_pandas()
    st.subheader("Lista de Petições Processadas")
    st.dataframe(petitions)
except Exception as e:
    st.error(f"Erro ao consultar dados no banco: {str(e)}")