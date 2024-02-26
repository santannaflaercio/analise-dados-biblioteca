from unidecode import unidecode
import pandas as pd
import glob

# Constantes
CSV_PATH = "data/dados_emprestimos"
PARQUET_PATH = "data/dados_exemplares.parquet"
CATEGORIAS = {
    range(0, 100): "GENERALIDADES - CIENCIA E CONHECIMENTO",
    range(100, 200): "FILOSOFIA E PSICOLOGIA",
    range(200, 300): "RELIGIAO",
    range(300, 400): "CIENCIAS SOCIAIS",
    range(400, 500): "CLASSE VAGA - PROVISORIAMENTE NAO OCUPADA",
    range(500, 600): "MATEMATICA E CIENCIAS NATURAIS",
    range(600, 700): "CIENCIAS APLICADAS",
    range(700, 800): "BELAS ARTES",
    range(800, 900): "LINGUAGEM - LINGUA - LINGUISTICA",
    range(900, 1000): "GEOGRAFIA - BIOGRAFIA - HISTORIA.",
}


def normalizar_texto(texto):
    """Normaliza strings: remove acentos e converte para maiúsculo."""
    return unidecode(texto).upper()


def carregar_dataframes(caminho):
    """Carrega todos os dataframes de arquivos CSV em um diretório."""
    todos_arquivos = glob.glob(caminho + "/*.csv")
    lista_df = [pd.read_csv(filename, index_col=None, header=0) for filename in todos_arquivos]
    return pd.concat(lista_df, axis=0, ignore_index=True)


def mesclar_dataframes(df1, df2, chave, tipo_mesclagem="inner"):
    """Mescla dois dataframes com base em uma chave comum."""
    return pd.merge(df1, df2, on=chave, how=tipo_mesclagem)


def limpar_dataframe(df):
    """Remove registros com valores nulos e normaliza colunas do tipo string."""
    df = df.dropna()
    df.loc[:, "matricula_ou_siape"] = df["matricula_ou_siape"].astype(int).astype(str)
    df.loc[:, df.select_dtypes(include=["object"]).columns] = df.select_dtypes(include=["object"]).map(normalizar_texto)
    df = df.drop(columns=["registro_sistema"])
    df = df.reset_index(drop=True)
    return df


def mapear_categoria(codigo):
    """Mapeia o código para a categoria correspondente."""
    for chave, valor in CATEGORIAS.items():
        if int(codigo) in chave:
            return valor
    return "CODIGO NAO MAPEADO"


def adicionar_categoria(df):
    """Adiciona a coluna 'categoria' ao dataframe."""
    df["categoria"] = df["localizacao"].apply(mapear_categoria)
    return df


# Execução principal
if __name__ == "__main__":
    try:
        df_unificado = carregar_dataframes(CSV_PATH)
        df_parquet = pd.read_parquet(PARQUET_PATH)
        df_mesclado = mesclar_dataframes(df_unificado, df_parquet, "codigo_barras")
        df_limpo = limpar_dataframe(df_mesclado)
        df_final = adicionar_categoria(df_limpo)
        print(df_final.head())
    except Exception as e:
        print(f"Ocorreu um erro: {e}")
