import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.sql.functions._

/**
 * Exercício de Spark
 * Por: Prof. Anderson  (Especialista em Dados e IA)
 * * Este código resolve os 3 maiores problemas de quem começa no Windows:
 * 1. Erros de NativeIO/Hadoop DLL
 * 2. Poluição de Logs no Terminal
 * 3. Integração de fontes distintas (CSV + Excel)
 * 
 * Objetivo do app: Receber dados de um arquivo CSV e de uma plailha excel, 
 *                  fazendo um join por uma chave (codigo_cliente) e 
 *                  montando uma saída agregando as informações complemen-
 *                  tares sobre o pagamento. Também são criadas colunas
 *                  com base em cálculos na própria linha, o que consome
 *                  um pouco mais de espaço, mas evita a sobrecarga de 
 *                  calculos por linha  no processamento de um grande
 *                  volume de linhas.
 *                  Também faz a gravação do relatório final em um arquivo CSV
 * 
 */
object PipelineGoldCompleto {
  def main(args: Array[String]): Unit = {

    // --- 1. CONFIGURAÇÕES DE AMBIENTE 
    System.setProperty("hadoop.home.dir", "C:/hadoop")
    
    // Código para evitar o erro UnsatisfiedLinkError: NativeIO$Windows.access0
    try {
      val loader = classOf[org.apache.hadoop.util.NativeCodeLoader]
      val field = loader.getDeclaredField("nativeCodeLoaded")
      field.setAccessible(true)
      field.set(null, false)
    } catch { case _: Exception => println("Aviso: Bypass NativeIO não aplicado.") }

    // --- 2. INICIALIZAÇÃO DA SESSÃO
    val spark = SparkSession.builder()
      .appName("PipelineGoldAnderson")
      .master("local[*]")
      .getOrCreate()

    // Reduzir as mensagens no log. Mesmo assim aparecem milhões. Vc vai até achar que são erros, mas não
    // são. É assim mesmo
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    println("🚀 Ambiente configurado. Lendo os dados...")

    try {
      // --- 3. LEITURA DE FONTES 

      // Lendo Vendas (CSV)
      val dfVendas = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("data/vendas.csv")

      // Lendo Pagamentos (Excel) - Requer a lib spark-excel no build.sbt
      val dfPagamentos = spark.read
        .format("com.crealytics.spark.excel")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("data/pagamentos.xlsx")

      // --- 4. TRATAMENTO E REGRAS DE NEGÓCIO 
      
      val dfFinal = dfVendas
        .join(dfPagamentos, Seq("codigo_cliente"), "left")
        // Tratando os nulos onde não houve pagamento
        .na.fill(0, Seq("total_pago")) 
        .withColumn("total_faturado", round($"total_faturado", 2))
        .withColumn("total_pago", round($"total_pago", 2))
        // Criando um indicador visual de status
        .withColumn("alerta", when($"total_pago" < $"total_faturado", "🚩 PENDENTE").otherwise("✅ PAGO"))

      // Exibindo o resultado para validação rápida
      println("--- Relatório Financeiro Consolidado ---")
      dfFinal.show()

      // --- 5. ESCRITA DOS DADOS ---
      dfFinal.write
        .mode(SaveMode.Overwrite)
        .option("header", "true")
        .csv("output/relatorio_final_gold")

      println("🏆 Processamento finalizado com sucesso!")

    } catch {
      case e: Exception => 
        println(s"❌ Erro no Pipeline: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}