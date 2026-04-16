package com.example.spark

/**
 * Parsed, catalog-qualified table identifier.
 *
 * The input is a dotted name like `production.features`. The first part(s) form the namespace
 * and the last part is the table. A catalog is prepended to produce a fully-qualified
 * reference that Spark SQL can resolve.
 */
private[spark] final case class TableIdentifier(
    catalog: String,
    namespace: Seq[String],
    table: String
) {
  import TableIdentifier.quote

  /** Fully-qualified identifier (e.g. `` `local`.`production`.`features` ``). */
  val qualifiedName: String = (catalog +: namespace :+ table).map(quote).mkString(".")

  /** Fully-qualified namespace (e.g. `` `local`.`production` ``), or `None` when the table has no namespace. */
  val qualifiedNamespace: Option[String] =
    if (namespace.isEmpty) None
    else Some((catalog +: namespace).map(quote).mkString("."))
}

private[spark] object TableIdentifier {

  def parse(catalog: String, dottedName: String): TableIdentifier = {
    require(catalog.nonEmpty, "Catalog must not be empty")
    val parts = dottedName.split("\\.").filter(_.nonEmpty).toSeq
    require(parts.nonEmpty, s"Table name must not be empty (got: '$dottedName')")
    TableIdentifier(catalog, parts.init, parts.last)
  }

  /** Quotes an identifier using backticks, escaping any embedded backtick. */
  def quote(identifier: String): String = s"`${identifier.replace("`", "``")}`"
}
