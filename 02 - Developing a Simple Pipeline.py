# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/Databricks-BR/lab_outubro_2025/refs/heads/main/Includes/images/handson_lab_outubro.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # 2 - Desenvolvendo um Pipeline Simples
# MAGIC
# MAGIC Nesta demonstração, criaremos um projeto simples de Lakeflow Declarative Pipeline usando o novo **editor multifile de Pipeline ETL** com SQL declarativo.
# MAGIC
# MAGIC ### Objetivos de Aprendizagem
# MAGIC
# MAGIC Ao final desta lição, você será capaz de:
# MAGIC - Descrever a sintaxe SQL usada para criar um Lakeflow Declarative Pipeline.
# MAGIC - Navegar pelo editor multifile de Pipeline ETL do Lakeflow para modificar configurações do pipeline e ingerir o(s) arquivo(s) de fonte de dados brutos.
# MAGIC - Criar, executar e monitorar um Lakeflow Declarative Pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC ## SELECIONE COMPUTE CLÁSSICO OU SERVERLESS
# MAGIC
# MAGIC Antes de executar as células neste notebook, selecione seu cluster de compute **Clássico** ou **Serverless**. O **Serverless** está habilitado por padrão, mas você pode escolher qualquer um dos dois.
# MAGIC
# MAGIC Siga estas etapas para selecionar o compute desejado:
# MAGIC
# MAGIC 1. No canto superior direito do notebook, clique no menu suspenso de compute.
# MAGIC 2. Selecione **Serverless** para usar compute serverless, ou selecione seu cluster **Clássico** se preferir.
# MAGIC 3. Se o cluster clássico não estiver listado:
# MAGIC     - Clique em **Mais** no menu suspenso.
# MAGIC     - Na janela pop-up **Anexar a um recurso de computação existente**, selecione o cluster desejado no primeiro menu.
# MAGIC 4. Se o cluster clássico estiver encerrado, reinicie-o:
# MAGIC     - No painel de navegação à esquerda, clique com o botão direito em **Compute** e selecione *Abrir em nova guia*.
# MAGIC     - Clique no ícone de triângulo ao lado do nome do cluster para iniciá-lo.
# MAGIC     - Aguarde até que o cluster esteja em execução e repita os passos acima para selecioná-lo.
# MAGIC
# MAGIC **NOTA:** Você pode alternar entre **Clássico** e **Serverless** a qualquer momento, conforme a necessidade do laboratório.

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Configuração do Laboratório
# MAGIC Abra o notebook [00_variaveis]($./00_variaveis) e redefina as variáveis de ambiente, caso necessário.
# MAGIC
# MAGIC <img src="Includes/images/variáveis.png" alt="Variáveis" style="max-width: 200px;">
# MAGIC
# MAGIC Execute a célula abaixo para configurar seu ambiente de trabalho para este curso.

# COMMAND ----------

# MAGIC %run ./00_setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Desenvolvendo e Executando um Pipeline Declarativo Lakeflow com o Editor Multifile de Pipeline ETL
# MAGIC
# MAGIC Este curso inclui um Pipeline Declarativo Lakeflow simples e pré-configurado para explorar e modificar. Nesta seção, iremos:
# MAGIC
# MAGIC - Explorar o editor multifile de Pipeline ETL e a sintaxe SQL declarativa  
# MAGIC - Modificar as configurações do pipeline  
# MAGIC - Executar o Pipeline Declarativo Lakeflow e explorar as tabelas streaming e a view materializada.

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Execute a célula abaixo e **copie o caminho** do output para o seu volume **academy.labs_lakeflow.raw_data**. Você precisará deste caminho ao modificar as configurações do seu pipeline.
# MAGIC
# MAGIC    Este caminho de volume contém os diretórios **customers_cdc**, **maintenance_logs**, **rides** e **weather**, que contêm os arquivos JSON brutos.
# MAGIC
# MAGIC    **EXEMPLO DE CAMINHO**: `/Volumes/seu_catalog/labs_lakeflow/raw_data`

# COMMAND ----------

# DBTITLE 1,Path to data source files
print(vol_path)

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Neste curso, temos arquivos iniciais para você usar em seu pipeline. Esta demonstração utiliza a pasta **2 - Desenvolvendo um Projeto de Pipeline Simples**. Para criar um pipeline e adicionar ativos existentes para associá-lo a arquivos de código já disponíveis em seu Workspace (incluindo pastas do Git), complete o seguinte:
# MAGIC
# MAGIC    a. Na barra de navegação à esquerda, selecione o ícone **Pasta** ![Folder Icon](./Includes/images/folder_icon.png) para abrir a navegação do Workspace.
# MAGIC
# MAGIC    b. Navegue até a pasta **Build Data Pipelines with Lakeflow Declarative Pipelines** (você pode já estar nela).
# MAGIC
# MAGIC    c. **(LEIA POR FAVOR)** Para facilitar, abra este mesmo notebook em uma guia separada:
# MAGIC
# MAGIC     - Clique com o botão direito no notebook na navegação à esquerda.
# MAGIC
# MAGIC     - Selecione **Abrir em uma Nova Guia**.
# MAGIC
# MAGIC    d. Na nova guia, clique no **ícone de três pontos (reticências)** ![Ellipsis Icon](./Includes/images/ellipsis_icon.png) na barra de navegação da pasta.
# MAGIC
# MAGIC    e. Selecione **Criar** → **ETL Pipeline**.
# MAGIC
# MAGIC    f. Complete a página de criação do pipeline com o seguinte:
# MAGIC
# MAGIC     - **Nome**: `Nomeie-seu-pipeline-usando-o-nome-deste-notebook-adicione-seu-primeiro-nome` 
# MAGIC     - **Catálogo padrão**: Selecione seu catálogo, definido em **catalog_name**  
# MAGIC     - **Schema padrão**: Selecione seu **schema** (banco de dados), definido em **schema_name**
# MAGIC
# MAGIC    g. Selecione **Adicionar ativos existentes**. No pop-up, complete o seguinte:
# MAGIC
# MAGIC     - **Pasta raiz do pipeline**: Selecione a pasta **lab_outubro_2025** (`/Workspace/Users/seu-nome-de-usuário-lab/lab_outubro_2025`)
# MAGIC
# MAGIC     - **Caminhos do código-fonte**: Dentro da mesma pasta raiz acima, selecione a pasta **transformations** (`/Workspace/Users/seu-nome-de-usuário-lab/lab_outubro_2025/transformations`)
# MAGIC
# MAGIC    h. Clique em **Adicionar**. Isso criará um pipeline e associará os arquivos corretos para esta demonstração.
# MAGIC
# MAGIC    i. Adicione as variáveis catalog e schema para reconhecer o volume criado. Em Settings, procure a sessão **Configuration**, clique em `Edit configuration` e adicione:
# MAGIC     - Key: **catalog** / Value: seu catálogo, definido em **catalog_name**  
# MAGIC     - Key: **schema** / Value: seu schema, definido em **schema_name**
# MAGIC
# MAGIC **Exemplo**
# MAGIC
# MAGIC ![Example Demo 2](./Includes/images/demo02_existing_assets.png)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Adicione Arquivos ao Armazenamento em Nuvem
# MAGIC
# MAGIC 1. Após explorar e executar o pipeline seguindo as instruções no arquivo **`01-bronze.sql`**, execute a célula abaixo para adicionar os primeiros arquivos ao seu volume em: `/Volumes/seu_catalogo/seu_schema/raw_data`.
# MAGIC
# MAGIC    **NOTA:** Se você receber o erro `name 'carga_dos_dados' is not defined`, será necessário executar novamente o script de configuração do laboratório no início deste notebook para criar o objeto `carga_dos_dados`. Isso é necessário para referenciar corretamente o caminho e copiar o arquivo com sucesso.
# MAGIC
# MAGIC    **NOTA:** Caso não possua conexão com o Github, os dados devem ser inseridos manualmente no Volume.
# MAGIC    Baixe os arquivos da pasta `Files`como ZIP, extrai o arquivo na sua máquina, entre na pasta `initial` e faça o upload das 4 subpastas.
# MAGIC    - Para isso, vá em Catálogo -> seu_catalogo -> seu_schema -> raw_data -> Upload to Volume 
# MAGIC    ![Ellipsis Icon](./Includes/images/upload_arquivos.png)
# MAGIC

# COMMAND ----------

# DBTITLE 1,Add a new JSON file to the data source
carga_dos_dados('initial')

# COMMAND ----------

# MAGIC %py
# MAGIC display(dbutils.fs.ls(f"{vol_path}/customers_cdc/"))

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Complete os seguintes passos para visualizar o novo arquivo em seu volume:
# MAGIC
# MAGIC    a. Selecione o ícone **Catálogo** ![Catalog Icon](./Includes/images/catalog_icon.png) no painel de navegação à esquerda.  
# MAGIC    
# MAGIC    b. Expanda o seu volume **seu_catalogo.seu_schema.raw_data**.  
# MAGIC    
# MAGIC    c. Expanda o diretório **customers_cdc**. Você deverá ver 3 arquivos em seu volume: 
# MAGIC     - **customers_cdc_2025-06-08.parquet** 
# MAGIC     - **customers_cdc_2025-06-09.parquet**
# MAGIC     - **customers_cdc_2025-06-10.parquet**

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Execute a célula abaixo para visualizar os dados no novo arquivo **/customers_cdc/customers_cdc_2025-06-08.parquet**. Observe o seguinte:
# MAGIC
# MAGIC    - O arquivo **customers_cdc_2025-06-08.parquet** contém novos dados.  
# MAGIC    - O arquivo **customers_cdc_2025-06-08.parquet** possui 22 linhas.

# COMMAND ----------

# DBTITLE 1,Preview the 01.json file
spark.sql(f'''
  SELECT *
  FROM PARQUET.`{vol_path}/customers_cdc/customers_cdc_2025-06-08.parquet`
''').display()

# COMMAND ----------

# MAGIC %md
# MAGIC 4. Execute a célula abaixo para visualizar os dados da pasta **/customers_cdc**. Observe o seguinte:
# MAGIC
# MAGIC    - O arquivo **customers_cdc_2025-06-08.parquet** possui 22 linhas.
# MAGIC    - O arquivo **customers_cdc_2025-06-09.parquet** possui 19 linhas.
# MAGIC    - O arquivo **customers_cdc_2025-06-10.parquet** possui 19 linhas.

# COMMAND ----------

spark.sql(f'''
  SELECT _metadata.file_name, *
  FROM PARQUET.`{vol_path}/customers_cdc/`
''').display()

# COMMAND ----------

# MAGIC %md
# MAGIC 4. Volte para o arquivo **01-bronze.sql** e selecione **Dry Run** para validar seu pipeline ETL com os arquivos gerados.
# MAGIC
# MAGIC    Observe a execução do pipeline e perceba o gráfico do pipeline será gerado, indicando a interação e dependências entre as tabelas. Porém nenhuma tabela foi criado e nenhum dado foi carregado.
# MAGIC    
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 5. Ainda no arquivo **01-bronze.sql** e selecione **Run Pipeline** para executar seu pipeline ETL.
# MAGIC
# MAGIC    Observe a execução do pipeline e perceba que o pipeline lida automativamente com o paralelismo na carga das tabelas.
# MAGIC
# MAGIC    Clique na caixa da tabela **customers_cdc_raw** para ter mais detalhes da execução dessa tabela. Em **Table Metrics** poderá ver que foram carregadas 60 linhas, assim como o conteúdo dos arquivos utilizados.
# MAGIC    
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Exploring Your Streaming Tables

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Visualize as novas tabelas de streaming e a view materializada em seu catálogo. Complete o seguinte:
# MAGIC
# MAGIC    a. Selecione o ícone de catálogo ![Ícone de Catálogo](./Includes/images/catalog_icon.png) no painel de navegação à esquerda.
# MAGIC
# MAGIC    b. Expanda seu catálogo **academy** (ou o nome que você definiu).
# MAGIC
# MAGIC    c. Expanda o schema **labs_lakeflow** (ou o nome que você definiu). Observe que as duas tabelas de streaming e a view materializada estão corretamente posicionadas em seus schemas.
# MAGIC
# MAGIC       - **academy.labs_lakeflow.maintenance_logs_raw** (bronze)
# MAGIC
# MAGIC       - **academy.labs_lakeflow.maintenance_logs** (silver)
# MAGIC
# MAGIC       - **academy.labs_lakeflow.maintenance_events** (gold)

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Execute a célula abaixo para visualizar os dados na tabela **{catalog_name}.{schema_name}.customers_cdc_raw**. Antes de executar a célula, quantas linhas essa tabela de streaming deve ter?
# MAGIC
# MAGIC    Observe o seguinte:
# MAGIC       - A tabela contém 60 linhas .
# MAGIC       - Na coluna **file_name** você pode ver exatamente de qual arquivo as linhas foram ingeridas.

# COMMAND ----------

# DBTITLE 1,View the streaming table
# MAGIC %py
# MAGIC spark.sql(f"""
# MAGIC SELECT *
# MAGIC FROM {catalog_name}.{schema_name}.customers_cdc_raw;
# MAGIC """).display()

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Complete os passos abaixo para visualizar o histórico da tabela de streaming **customers_cdc_raw**:
# MAGIC
# MAGIC    a. Selecione o ícone **Catálogo** ![Catalog Icon](./Includes/images/catalog_icon.png) no painel de navegação à esquerda.  
# MAGIC    
# MAGIC    b. Expanda o schema **{catalog_name}.{schema_name}**.  
# MAGIC    
# MAGIC    c. Clique no ícone de três pontos (reticências) ao lado da tabela **customers**.  
# MAGIC    
# MAGIC    d. Selecione **Abrir no Catalog Explorer**.  
# MAGIC    
# MAGIC    e. No Catalog Explorer, selecione a guia **Histórico**. Observe que um erro pode ser retornado porque visualizar o histórico de uma tabela de streaming requer **SHARED_COMPUTE**. 
# MAGIC
# MAGIC    f. Acima dos seus catálogos à esquerda, selecione seu cluster de computação SQL.
# MAGIC
# MAGIC    ![Change Compute](./Includes/images/change_compute.png)  
# MAGIC    
# MAGIC    g. Volte e observe as duas últimas versões da tabela. Observe o seguinte:  
# MAGIC    
# MAGIC       - Na coluna **Operation**, as duas últimas atualizações foram **STREAMING UPDATE**.  
# MAGIC       
# MAGIC       - Expanda os valores de **Operation Parameters** das duas últimas atualizações. Note que ambas usam `"outputMode": "Append"`.  
# MAGIC       
# MAGIC       - Encontre a coluna **Operation Metrics**. Expanda os valores das duas últimas atualizações. Observe o seguinte:
# MAGIC       
# MAGIC          - São exibidas várias métricas para a atualização de streaming: **numRemovedFiles, numOutputRows, numOutputBytes e numAddedFiles**.  
# MAGIC          
# MAGIC          - Nos valores de `numOutputRows`, 60 linhas foram adicionadas na primeira atualização.
# MAGIC    
# MAGIC    h. Feche o Catalog Explorer.

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Visualizando Pipelines Declarativos Lakeflow com a UI de Pipelines
# MAGIC
# MAGIC Após explorar e criar seu pipeline usando o arquivo **01-bronze.sql** nos passos acima, você pode visualizar os pipelines criados em seu workspace através da interface **Pipelines**.

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Complete os seguintes passos para visualizar o pipeline que você criou:
# MAGIC
# MAGIC    a. No painel de navegação principal à esquerda (você pode precisar expandi-lo selecionando o ícone ![Expandir Painel de Navegação](./Includes/images/expand_main_navigation.png) no canto superior esquerdo do seu workspace), clique com o botão direito em **Jobs & Pipelines** e selecione **Abrir Link em Nova Guia**.
# MAGIC
# MAGIC    b. Isso deve levar você aos pipelines que você criou. Você deve ver seu pipeline **2 - Desenvolvendo um Projeto de Pipeline Simples - <seu usuário/nome>**.
# MAGIC
# MAGIC    c. Selecione seu **2 - Desenvolvendo um Projeto de Pipeline Simples - <seu usuário/nome>**. Aqui, você pode usar a interface para modificar o pipeline.
# MAGIC
# MAGIC    d. Selecione o botão **Settings** no topo. Isso o levará para as configurações dentro da interface.
# MAGIC
# MAGIC    e. Selecione **Schedule** para agendar o pipeline. Selecione **Cancel**, aprenderemos a agendar o pipeline depois.
# MAGIC
# MAGIC    f. Abaixo do nome do seu pipeline, selecione o menu suspenso com o carimbo de data e hora. Aqui você pode visualizar o **Gráfico do Pipeline** e outras métricas para cada execução do pipeline.
# MAGIC
# MAGIC    g. Feche a guia da interface do pipeline que você abriu.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recursos Adicionais
# MAGIC
# MAGIC - [Documentação do Lakeflow Declarative Pipelines](https://docs.databricks.com/aws/pt/dlt/).

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
