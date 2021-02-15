package autoreLibro

import org.apache.spark.{SparkConf, SparkContext}

import scala.Console.println

object Main {
  def main(args: Array[String]): Unit = {
    val autore1 = Autore(1, "mario", "rossi", 36)
    val autore2 = Autore(2, "marco", "verdi", 42)

    val libro1 = Libro(1, "libro1", 234, "1960", 1)
    val libro2 = Libro(2, "libro2", 544, "1967", 1)
    val libro3 = Libro(3, "libro3", 164, "1979", 1)
    val libro4 = Libro(4, "libro4", 756, "1967", 2)
    val libro5 = Libro(5, "libro5", 121, "1977", 2)


    val autori = Array(autore1, autore2)
    val libri = Array(libro1, libro2, libro3, libro4, libro5, libro3)

    val configurazioneSpark = new SparkConf().setMaster("local[2]").setAppName("CountingSheep")
    val contextSpark = new SparkContext(configurazioneSpark)
    contextSpark.setCheckpointDir("C:\\Users\\Corsista\\Desktop\\BigData\\checkpoint")

    val rddAutori = contextSpark.parallelize(autori)
    val rddLibri = contextSpark.parallelize(libri)

    val libriPair = rddLibri.map(x => (x.idAutore, x))
    val ragrupatto = libriPair.groupByKey()
    ragrupatto.foreach(println)


    // LIBRI RAGRUPPATI PER AUTORE
    val ragrupPerAutore = libriPair.groupByKey() //UTILIZZO groupByKey
    ragrupPerAutore.foreach(println)
    //SOMMA PAGINE PER AUTORE

    val newLibriPair = libriPair

    def funMaxPage = (libro1: Libro, libro2: Libro) => {
      if (libro1.numeroPagine > libro2.numeroPagine) {
        libro1
      } else {
        libro2
      }
    }

    val libroMaxPage = newLibriPair.reduceByKey(funMaxPage) //UTILIZZO reduceByKey
    libroMaxPage.foreach(println)
    //______________________________________________________

    def seqOp = (pagine: Int, libro: Libro) => {
      pagine + libro.numeroPagine
    }

    def combOp = (pagine1: Int, pagine2: Int) => {
      pagine1 + pagine2
    }

    println("totale per autore")
    val totalePerAutore = newLibriPair.aggregateByKey(0)(seqOp, combOp)
    totalePerAutore.checkpoint()
    totalePerAutore.foreach(println)
    //----------------------------------------------------------------------------------
    //---------------------TERZZA CARRELLATA DI ESERCIZZI-------------------------------
    //----------------------------------------------------------------------------------

//_____________________TUPLA libroRDD (idLibro,idAutore,dataDiPubblicazione)____________
    val tuplaLibri = rddLibri.map(x => (x.idLibro, x.idAutore, x.annoPublicazione))
    println("TUPLA libroRDD (idLibro,idAutore,dataDiPubblicazione)")
    tuplaLibri.foreach(println)
    //___________________________________senza duplicati________________________________
    val tuplaLibriSenzaDuplicati = tuplaLibri.distinct()
    println("senza duplicati")
    tuplaLibriSenzaDuplicati.foreach(println)

    //_________________RIMUOVERE gli elementi con anno publicazione dopo 1978___________
    val tuplaFiltrata = tuplaLibri.filter(x => x._3 < "1978")
    println("RIMUOVERE gli elementi con anno publicazione dopo 1978")
    tuplaFiltrata.foreach(println)

    //__________________________________primo libro publicato per autore___________________________
    def funFistBook = (libro1: Libro, libro2: Libro) => {
      if (libro1.annoPublicazione < libro2.annoPublicazione) {
        libro1
      } else {
        libro2
      }
    }

    val primaPublicazione = libriPair.reduceByKey(funFistBook)
    println("primo libro publicato per autore")
    primaPublicazione.foreach(println)

  }
}
