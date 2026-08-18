# 🚀 Spark Gold Pipeline - Scala & Windows Edition

Este foi feito em linguagem Scala, usada nas clouds lideres de mercado. Eu tive a idéia de fazer quando me aventurei pelo Databricks, e nos primeiros testes consumiu todos os créditos antes de eu aprender a lidar com o ambiente e a linguagem. Então configurei um ambiente local, com o sbt (Scala Build Tool), para testar tudo local, sem precisar da plataforma, até o código ficar maduro e não gastar tokens sem necessidade.

Ele consome dados de várias fontes, faz a integração, e entrega um dataset consolidado e estruturado.


Este projeto demonstra a construção de um pipeline de dados (camada Gold) utilizando **Apache Spark** e **Scala**, superando os desafios comuns de configuração em ambiente Windows.

### ✅ O que este projeto resolve:
- **Compatibilidade Windows:** Configuração de `Winutils` e correção de erros de `NativeIO`.
- **Gestão de Dependências:** Uso do `build.sbt` para integrar bibliotecas de leitura de Excel e Spark SQL.
- **Java 17+ Compatibility:** Ajustes de `--add-opens` para garantir que o Spark acesse os módulos internos do Java sem erros de permissão.
- **Data Cleaning Avançado:** Tratamento de valores nulos e padronização de esquemas de dados.

### 🛠️ Tecnologias e Ferramentas
- **Linguagem:** Scala
- **Framework:** Apache Spark 3.x
- **Build Tool:** sbt (Scala Build Tool)
- **Ambiente:** VS Code com extensão Metals
- **Controle de Versão:** Git & GitHub

### 📂 Estrutura do Projeto
- `src/main/scala/`: Código fonte do pipeline.
- `data/`: Arquivos de entrada (CSV, XLSX) — *Nota: arquivos ignorados no .gitignore para segurança.*
- `build.sbt`: Configuração das bibliotecas e dependências do Spark.

### 🚀 Como Rodar o Projeto
1. Certifique-se de ter o **Java 17** e o **sbt** instalados.
2. Clone o repositório:
   ```bash
   git clone [https://github.com/SEU_USUARIO/NOME_DO_REPO.git](https://github.com/SEU_USUARIO/NOME_DO_REPO.git)
