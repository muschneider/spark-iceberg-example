package com.example.spark

final private[spark] case class TableIdentifier(catalog: String, namespace: Seq[String], table: String) {

    import TableIdentifier.quote

    val qualifiedName: String = (catalog +: namespace :+ table).map(quote).mkString(".")

    val qualifiedNamespace: Option[String] =
        if (namespace.isEmpty)
            None
        else
            Some((catalog +: namespace).map(quote).mkString("."))

}

private[spark] object TableIdentifier {

    def parse(catalog: String, dottedName: String): TableIdentifier = {
        require(catalog.nonEmpty, "Catalog must not be empty")
        val parts = dottedName.split("\\.").filter(_.nonEmpty).toSeq
        require(parts.nonEmpty, s"Table name must not be empty (got: '$dottedName')")
        TableIdentifier(catalog, parts.init, parts.last)
    }

    def quote(identifier: String): String = s"`${identifier.replace("`", "``")}`"
}
