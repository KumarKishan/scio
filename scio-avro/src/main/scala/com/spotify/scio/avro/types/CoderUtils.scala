/*
 * Copyright 2017 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.scio.avro.types

import org.apache.avro.specific.SpecificRecordBase
import scala.reflect.macros._

private[scio] object CoderUtils {


  /**
  * Generate a coder which does not serializa the schema and relies exclusively on type.
  */
  def staticInvokeCoder[T <: SpecificRecordBase : c.WeakTypeTag](c: blackbox.Context): c.Tree = {
    import c.universe._
    val wtt = weakTypeOf[T]
    val companioned = wtt.typeSymbol
    val companionSymbol = companioned.companion
    val companionType = companionSymbol.typeSignature

    q"""
    _root_.com.spotify.scio.coders.Coder.beam(
      new _root_.com.spotify.scio.coders.AvroRawCoder[$companioned](${companionType}.getClassSchema())
    )
    """
  }

  var verbose = true
  val reported: scala.collection.mutable.Set[(String, String)] =
    scala.collection.mutable.Set.empty

  def issueFallbackWarning[T: c.WeakTypeTag](c: whitebox.Context)(lp: c.Expr[shapeless.LowPriority]): c.Tree = {
    import c.universe._
    val wtt = weakTypeOf[T]
    val TypeRef(pre, sym, args) = wtt
    val companioned = wtt.typeSymbol

    val typeName = sym.name
    val params = args.headOption.map { _ => args.mkString("[", ",", "]") }.getOrElse("")
    val fullType = typeName + params

    // Use shapeless magic to find out if a proper implicit is actually available
    val lowPrio = new shapeless.LowPriorityMacros(c)
    val secondImplicit = lowPrio.secondOpenImplicitTpe
    val implicitFound = secondImplicit.isDefined

    val toReport = (c.enclosingPosition.toString -> wtt.toString)
    val alreadyReported = reported.contains(toReport)
    if(!alreadyReported) reported += toReport

    def shortMessage =
      s"""
      | Warning: No implicit Coder found for type:
      |
      |   >> $wtt
      """

    def longMessage =
      shortMessage +
        s"""
        |
        |  Scio will use a fallback Kryo coder instead.
        |  Most types should be supported out of the box by simply importing `com.spotify.scio.coders.Implicits._`.
        |  If a type is not supported, consider implementing your own implicit Coder for this type.
        |  It is recommended to declare this Coder in your class companion object:
        |
        |       object ${typeName} {
        |         import com.spotify.scio.coders.Coder
        |         import org.apache.beam.sdk.coders.AtomicCoder
        |
        |         implicit def coder${typeName}: Coder[$fullType] =
        |           Coder.beam(new AtomicCoder[$fullType] {
        |             def decode(in: InputStream): $fullType = ???
        |             def encode(ts: $fullType, out: OutputStream): Unit = ???
        |           })
        |       }
        |
        |  If you do want to use a Kryo coder, be explicit about it:
        |
        |       implicit def coder${typeName}: Coder[$fullType] = Coder.fallback[$fullType]
        |
        """

    val fallback = q"""_root_.com.spotify.scio.coders.Coder.fallback[$wtt]"""

    (verbose, alreadyReported) match {
      case _ if implicitFound =>
        fallback
      case (true, false) =>
        c.echo(c.enclosingPosition, longMessage.stripMargin)
        verbose = false
        fallback
      case (false, false) =>
        c.echo(c.enclosingPosition, shortMessage.stripMargin)
        fallback
      case (_, _) =>
        fallback
    }
  }

  // Add a level of indirection to prevent the macro from capturing
  // $outer which would make the Coder serialization fail
  def wrappedCoder[T: c.WeakTypeTag](c: whitebox.Context): c.Tree = {
    import c.universe._
    val wtt = weakTypeOf[T]
    val companioned = wtt.typeSymbol

    if(wtt <:< typeOf[Seq[_]])
      c.abort(c.enclosingPosition, s"Automatic coder derivation can't derive a Coder for $wtt <: Seq")

    val magTree = magnolia.Magnolia.gen[T](c)

    def getLazyVal =
      magTree match {
        case q"lazy val $name = $body; $rest" =>
          body
      }

    val name = c.freshName(s"$$ShimDerivedCoder")
    val className = TypeName(name)
    val termName = TermName(name)

    // Remove annotations from magnolia since they are not serialiazable and we don't use them anyway
    val removeAnnotations =
      new Transformer {
        override def transform(tree: Tree) =
          tree match {
            case Apply(TypeApply(Select(Select(_, TermName("Magnolia")), TermName("caseClass")), _), params @ List(name, isObj, isVal, ps, _, construct)) =>
              q"_root_.magnolia.Magnolia.caseClass($name, $isObj, $isVal, $ps, scala.Array(), $construct)"
            case q"com.spotify.scio.coders.Implicits.dispatch(new magnolia.SealedTrait($name, $subtypes, $annotations))" =>
              q"_root_.com.spotify.scio.coders.Implicits.dispatch(new magnolia.SealedTrait($name, $subtypes, Array()))"
            case q"magnolia.Magnolia.param[$tc, $t, $pt]($name, $isRepeated, $typeclass, $default, $f, $annotations)" =>
              val tcname = c.freshName(s"paramTypeclasssss")
              val tcTermName = TermName(tcname)
              q"""
              val $tcTermName = $typeclass
              _root_.magnolia.Magnolia.param[$tc, $t, $pt]($name, $isRepeated, $tcTermName, $default, $f, Array())
              """
            case t =>
              super.transform(tree)
          }
      }

    val coder = removeAnnotations.transform(getLazyVal)

    //XXX: find a way to get rid of $outer references at compile time
    val tree: c.Tree = coder
    tree
  }

}
