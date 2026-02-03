import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.sql.functions._

/**
 * PIPELINE GOLD: O Guia de Sobrevivência para Iniciantes (Windows Edition)
 * Criado por: Anderson de Faria Pinto
 * Objetivo: Vencer as barreiras de ambiente e processar dados com sucesso.
 */
object SparkWindowsFix {
  def main(args: Array[String]): Unit = {
    
    // 1. O MAPA DA MINA: Define onde o Hadoop está (essencial no Windows)
    System.setProperty("hadoop.home.dir", "C:/hadoop")

    // 2. O HACK DE INFRA: Se não tens a hadoop.dll no System32, este bloco 
    // desativa a verificação nativa do Windows que causa o erro 'access0'.
    try {
      val loader = classOf[org.apache.hadoop.util.NativeCodeLoader]
      val field = loader.getDeclaredField("nativeCodeLoaded")
      field.setAccessible(true)
      field.set(null, false)
      println("✅ Hack de NativeIO aplicado: Bypass de permissões Windows ativo.")
    } catch { case _: Exception => println("⚠️ Falha ao aplicar hack, mas seguimos!") }

    // 3. O SILÊNCIO DOS LOGS: Evita que o terminal te atropele com INFOs inúteis.
    val spark = SparkSession.builder()
      .appName("Desafio Spark Gold")
      .master("local[*]") // Usa todos os núcleos do teu PC
      .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    println("🚀 Spark iniciado com sucesso. Processando...")

    // --- LOGICA DE EXEMPLO (A que fizemos hoje) ---
    // Criando dados de exemplo para teste rápido
    val df = Seq(
      (101, 1500.0, 1600.0),
      (102, 2300.5, null.asInstanceOf[Double]), // O temido valor nulo
      (103, 800.0, 800.0)
    ).toDF("id", "faturado", "pago")

    // Tratamento de NULLs e Arredondamento (A dopamina do dado limpo!)
    val dfFinal = df.na.fill(0.0)
      .withColumn("pago", round($"pago", 2))
      .withColumn("status", when($"pago" === 0, "🚩 REVISAR").otherwise("✅ OK"))

    dfFinal.show()

    // 4. O CARIMBO FINAL: Salvando sem erro de Permissão
    dfFinal.write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv("output/relatorio_sucesso")

    println("🏆 Se leste isto, tu venceste o Windows e o Spark!")
    spark.stop()
  }
}