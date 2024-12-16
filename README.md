# Projeto Apache Airflow & Spark

Este repositório é destinado à orquestração de aplicações Spark utilizando a ferramenta Apache Airflow e ao armazenamento de arquivos no AWS S3. O objetivo é implementar uma arquitetura de dados baseada no modelo Lakehouse (camadas **landing**, **bronze**, **silver** e **gold**) com transformações feitas em PySpark.

---

## Índice

1. [Tecnologias Utilizadas](#tecnologias-utilizadas)  
2. [Requisitos e Configurações](#requisitos-e-configurações)  
3. [Execução do Projeto](#execução-do-projeto)  
4. [Arquitetura Lakehouse](#arquitetura-lakehouse)  
5. [Padronização de Commits](#padronização-de-commits)

---

## Tecnologias Utilizadas

- **Python**: Versão 3.12  
- **PySpark**: Versão 3.5.3  
- **Apache Airflow**: Versão 2.5.3  
- **Amazon AWS S3**: Armazenamento de dados  
- **Ubuntu Linux**: Ambiente operacional utilizado

---

## Requisitos e Configurações

### Instalação das Dependências

1. **Clonar o repositório:**

   ```bash
   git clone git@github.com:seuusuario/ProjetoAirflowSpark.git
   cd ProjetoAirflowSpark
   ```

2. **Criar e ativar um ambiente virtual:**

   - **Linux/MacOS:**
     ```bash
     python3 -m venv venv
     source venv/bin/activate
     ```
   - **Windows:**
     ```bash
     python -m venv venv
     venv\Scripts\activate
     ```

3. **Instalar os pacotes necessários:**

   ```bash
   pip install -r requirements.txt
   ```

4. **Configurar o Apache Airflow:**

   Caso seja a primeira execução do Airflow na sua máquina, inicialize o banco de dados e crie o usuário administrador:

   ```bash
   airflow db init
   airflow users create \
       --username admin \
       --firstname Admin \
       --lastname User \
       --role Admin \
       --email admin@email.com \
       --password sua_senha
   ```

5. **Configurar as DAGs no Airflow:**

   Edite o arquivo de configuração `airflow.cfg` para alterar o caminho padrão das DAGs:

   ```bash
   dags_folder = /caminho/para/seu/repositório/dags
   ```

6. **Configurar as credenciais AWS no Airflow:**

   - Acesse o menu **Admin > Connections**.  
   - Encontre a conexão `aws_default` e configure os seguintes campos:
     - **Access Key**: Sua chave de acesso.
     - **Secret Key**: Sua chave secreta.
   - Adicione também as seguintes variáveis em **Admin > Variables**:
     - `AWS_ACCESS_KEY_ID`
     - `AWS_SECRET_ACCESS_KEY`
     - `AWS_SESSION_TOKEN` (caso necessário).

---

## Execução do Projeto

1. **Iniciar o Apache Airflow:**

   Abra dois terminais:
   - No primeiro terminal, execute o servidor web:
     ```bash
     airflow webserver --port 8080
     ```
   - No segundo terminal, execute o scheduler:
     ```bash
     airflow scheduler
     ```
     - Ou utilize a opção:
     ```bash
      airflow standalone
     ```

2. **Acessar a interface do Airflow:**

   Abra o navegador e acesse: [http://localhost:8080](http://localhost:8080).  
   Efetue o login com as credenciais criadas anteriormente.

3. **Executar a DAG:**

   - Procure pela DAG desejada na interface.
   - Clique no botão **Trigger DAG** para iniciar a execução.
   - Acompanhe o progresso das tarefas pela visualização gráfica.

---

## Arquitetura Lakehouse

A arquitetura Lakehouse segue o padrão **Medallion** e é dividida em três camadas principais:

1. **Landing:** Dados brutos extraídos de sistemas externos.  
2. **Bronze:** Dados transformados para um formato otimizado (Parquet).  
3. **Silver:** Dados limpos e com colunas renomeadas para facilitar análises.  
4. **Gold:** Dados agregados e preparados para análises mais profundas, como relatórios e dashboards.

---

## Padronização de Commits

Para manter a organização do histórico de commits, utilize a seguinte convenção:

- **feat:** Para novos recursos.  
- **fix:** Para correção de bugs.  
- **refac:** Para refatoramento de código.  
- **docs:** Para alterações na documentação (como este README).  
- **test:** Para modificações em testes.

Exemplo de commit:

```bash
feat: adicionar transformação da camada gold no Lakehouse
```

---

## Observação Importante

Este repositório **não inclui** o diretório de instalação do Spark (`spark-3.5.3-bin-hadoop3`). Certifique-se de instalar esta versão antes de executar o projeto. O Spark pode ser baixado [aqui](https://spark.apache.org/downloads.html).

