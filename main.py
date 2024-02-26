from unidecode import unidecode
import pandas as pd
import glob

# Constantes
CSV_PATH = "data/dados_emprestimos"
PARQUET_PATH = "data/dados_exemplares.parquet"


def normalizar_texto(texto):
    """
    Normaliza strings: remove acentos e converte para maiúsculo.
    """
    return unidecode(texto).upper()


def carregar_dataframes(caminho):
    """
    Carrega todos os dataframes de arquivos CSV em um diretório.
    """
    todos_arquivos = glob.glob(caminho + "/*.csv")
    lista_df = []
    for filename in todos_arquivos:
        df = pd.read_csv(filename, index_col=None, header=0)
        lista_df.append(df)
    return pd.concat(lista_df, axis=0, ignore_index=True)


def mesclar_dataframes(df1, df2, chave, tipo_mesclagem="inner"):
    """
    Mescla dois dataframes com base em uma chave comum.
    """
    return pd.merge(df1, df2, on=chave, how=tipo_mesclagem)


def limpar_dataframe(df):
    """
    Remove registros com valores nulos e normaliza colunas do tipo string.
    """
    df_limpo = df.dropna()
    df_limpo["matricula_ou_siape"] = df_limpo["matricula_ou_siape"].apply(lambda x: int(float(x)))
    for coluna in df_limpo.select_dtypes(include=["object"]).columns:
        df_limpo.loc[:, coluna] = df_limpo[coluna].apply(normalizar_texto)
    return df_limpo


# Execução principal
if __name__ == "__main__":
    try:
        df_unificado = carregar_dataframes(CSV_PATH)
        df_parquet = pd.read_parquet(PARQUET_PATH)
        df_mesclado = mesclar_dataframes(df_unificado, df_parquet, "codigo_barras")
        df_limpo = limpar_dataframe(df_mesclado)
        print(df_limpo.head())
    except Exception as e:
        print(f"Ocorreu um erro: {e}")
