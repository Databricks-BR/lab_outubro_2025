# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://raw.githubusercontent.com/Databricks-BR/lab_outubro_2025/refs/heads/main/Includes/images/handson_lab_outubro.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # 1 - Ingestão de Dados com LakeFlow Connect
# MAGIC
# MAGIC Nesta demonstração, você será apresentado aos conectores gerenciados **LakeFlow Connect** para fontes empresariais externas. Uma ferramenta fácil de usar para trazer dados de diversos sistemas, como bancos de dados (por exemplo, SQL Server), aplicativos (por exemplo, Salesforce, Workday) e serviços de armazenamento de arquivos (por exemplo, SharePoint) para o lakehouse.
# MAGIC
# MAGIC Ela foi projetada para lidar com dados de forma eficiente e melhorar automaticamente o desempenho.
# MAGIC
# MAGIC ### Objetivos de Aprendizagem
# MAGIC
# MAGIC Ao final da demonstração, você deverá ser capaz de:
# MAGIC
# MAGIC - Explorar os conectores gerenciados pela Databricks disponíveis para ingerir dados via LakeFlow Connect.
# MAGIC - Ver como ingerir dados usando o Partner Connect.
# MAGIC - Adicionar um conector de banco de dados e configurar uma demonstração de pipeline de ingestão:
# MAGIC   - Escolher quais dados você deseja ingerir e sincronizar com a Databricks
# MAGIC   - Sincronizar o pipeline de dados com o Unity Catalog (catálogo e esquema)

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Complete as etapas a seguir para visualizar a documentação do Lakeflow Connect:
# MAGIC
# MAGIC    a. Acesse a página da [documentação Databricks](https://docs.databricks.com/aws/en/).
# MAGIC
# MAGIC    b. Na barra de navegação à esquerda, expanda **Data Engineering**.
# MAGIC
# MAGIC    c. Expanda **Lakeflow Connect** para visualizar a documentação disponível sobre ingestão de dados com Lakeflow Connect.
# MAGIC
# MAGIC    d. Expanda **Managed Connectors** para acessar informações sobre conectores gerenciados pela Databricks.
# MAGIC
# MAGIC **NOTA:** As instruções de navegação na documentação podem mudar após uma atualização.

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Complete as etapas a seguir para explorar as capacidades de **Ingestão de Dados** disponíveis no Databricks:
# MAGIC
# MAGIC    a. Na barra de navegação principal à esquerda, clique com o botão direito em **Ingestão de Dados** e selecione **Abrir em uma Nova Guia**.
# MAGIC
# MAGIC    b. Em **Conectores Databricks**, você verá vários conectores gerenciados pela Databricks fornecidos pelo LakeFlow Connect.
# MAGIC
# MAGIC    c. Na seção **Arquivos**, você pode:
# MAGIC    
# MAGIC    - **Criar ou modificar tabelas**
# MAGIC
# MAGIC    - **Fazer upload de arquivos para um volume**
# MAGIC
# MAGIC    - **Criar uma tabela a partir do Amazon S3**
# MAGIC    
# MAGIC    - Arrastar e soltar arquivos ao usar as opções para criar/modificar tabelas ou fazer upload de arquivos para um volume.
# MAGIC
# MAGIC    d. Em **Conectores Fivetran**, você pode pesquisar conexões de fontes de dados específicas usando um parceiro.
# MAGIC
# MAGIC    e. Clique em qualquer conector para ver os detalhes.
# MAGIC
# MAGIC    f. Você também pode fazer upload de arquivos diretamente para o **DBFS** para compatibilidade retroativa, embora o Databricks recomende fazer upload para o Unity Catalog daqui para frente.

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Complete a demonstração a seguir para aprender como usar um conector gerenciado Databricks para um banco de dados externo ou aplicativo SaaS usando o **LakeFlow Connect**.
# MAGIC
# MAGIC       **NOTA:** Este curso não possui um banco de dados ou aplicativo SaaS ativo para uso. As demonstrações abaixo são apenas um passo a passo. Durante uma aula ao vivo, escolha um dos tours abaixo.
# MAGIC
# MAGIC    - [Demonstração de Conector Gerenciado LakeFlow Connect](https://app.getreprise.com/launch/BXZY58n/) - Demonstração simples
# MAGIC
# MAGIC    - [Databricks Lakeflow Connect para Relatórios Workday: Conecte, Ingerira e Analise Dados Workday Sem Complexidade](https://www.databricks.com/resources/demos/tours/appdev/lakeflow-workday-connect?itm_data=demo_center)
# MAGIC
# MAGIC    - [Databricks Lakeflow Connect para Salesforce: Potencializando Vendas Inteligentes com IA e Analytics](https://www.databricks.com/resources/demos/tours/platform/discover-databricks-lakeflow-connect-demo?itm_data=demo_center)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
